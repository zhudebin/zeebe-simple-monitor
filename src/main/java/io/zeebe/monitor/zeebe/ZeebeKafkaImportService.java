package io.zeebe.monitor.zeebe;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.proto.Schema.Record;
import io.zeebe.exporters.kafka.serde.RecordId;
import io.zeebe.exporters.kafka.serde.RecordIdDeserializer;
import io.zeebe.exporters.kafka.serde.RecordProtobufDeserializer;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "zeebe.exporter", havingValue = "kafka")
public class ZeebeKafkaImportService {

  @Value("${zeebe.client.worker.kafka.consumer.topicPattern:'^zeebe.*$'}")
  private String topicPattern;
  @Value("${zeebe.client.worker.kafka.consumer.servers}")
  private String servers;
  @Value("${zeebe.client.worker.kafka.consumer.groupId}")
  private String groupId;
  @Value("${zeebe.client.worker.kafka.consumer.config}")
  private String propertiesStringConfig;

  private static final Logger LOG = LoggerFactory.getLogger(ZeebeKafkaImportService.class);

  private static final List<Class<? extends Message>> RECORD_MESSAGE_TYPES;

  static {
    RECORD_MESSAGE_TYPES = new ArrayList<>();
    RECORD_MESSAGE_TYPES.add(Schema.DeploymentRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.DeploymentDistributionRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ErrorRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.IncidentRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.JobRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.JobBatchRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.MessageStartEventSubscriptionRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.MessageSubscriptionRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.MessageRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ProcessRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ProcessEventRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ProcessInstanceRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ProcessInstanceCreationRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.ProcessMessageSubscriptionRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.TimerRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.VariableRecord.class);
    RECORD_MESSAGE_TYPES.add(Schema.VariableDocumentRecord.class);
  }

  private final Map<Class<?>, List<Consumer<?>>> listeners = new HashMap<>();

  @Autowired
  private ImportService importService;

  @PostConstruct
  public void start() {
    initListener();
    importFrom();
  }

  @Async
  public void importFrom() {

    final Map<String, Object> config = getConfig();

    final org.apache.kafka.clients.consumer.Consumer<RecordId, Record> consumer =
        new KafkaConsumer(config, new RecordIdDeserializer(), new RecordProtobufDeserializer());
    Pattern subscriptionPattern = Pattern.compile(topicPattern);
    consumer.subscribe(subscriptionPattern);

    while (true) {
      final ConsumerRecords<RecordId, Record> consumed =
          consumer.poll(Duration.ofSeconds(1));
      for (final ConsumerRecord<RecordId, Record> record : consumed) {

        for (Class<? extends com.google.protobuf.Message> type : RECORD_MESSAGE_TYPES) {
          final boolean handled;
          try {
            handled = handleRecord(record.value(), type);
            if (handled) {
              break;
            }
          } catch (InvalidProtocolBufferException e) {
            LOG.error("", e);
          }
        }

        LOG.debug(
            "================[{}] {}-{} ================",
            record.topic(),
            record.key().getPartitionId(),
            record.key().getPosition());
        LOG.debug("{}", record.value());
      }
    }


  }

  private Map<String, Object> getConfig() {
    final Map<String, Object> config = new HashMap<>();
    // default
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.MAX_VALUE);
    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5_000);
    // overwrite
    config.putAll(parseProperties(propertiesStringConfig));
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return config;
  }

  private void initListener() {
    addListener(Schema.ProcessRecord.class,
        record -> ifEvent(record, Schema.ProcessRecord::getMetadata, importService::importProcess));
    addListener(Schema.ProcessInstanceRecord.class,
        record -> ifEvent(record, Schema.ProcessInstanceRecord::getMetadata,
            importService::importProcessInstance));
    addListener(Schema.IncidentRecord.class,
        record -> ifEvent(record, Schema.IncidentRecord::getMetadata,
            importService::importIncident));
    addListener(Schema.JobRecord.class,
        record -> ifEvent(record, Schema.JobRecord::getMetadata, importService::importJob));
    addListener(Schema.VariableRecord.class,
        record -> ifEvent(record, Schema.VariableRecord::getMetadata,
            importService::importVariable));
    addListener(Schema.TimerRecord.class,
        record -> ifEvent(record, Schema.TimerRecord::getMetadata, importService::importTimer));
    addListener(Schema.MessageRecord.class,
        record -> ifEvent(record, Schema.MessageRecord::getMetadata, importService::importMessage));
    addListener(Schema.MessageSubscriptionRecord.class,
        record -> ifEvent(record, Schema.MessageSubscriptionRecord::getMetadata,
            importService::importMessageSubscription));
    addListener(Schema.MessageStartEventSubscriptionRecord.class,
        record -> ifEvent(record, Schema.MessageStartEventSubscriptionRecord::getMetadata,
            importService::importMessageStartEventSubscription));
    addListener(Schema.ErrorRecord.class, importService::importError);
  }

  private <T extends com.google.protobuf.Message> void addListener(Class<T> recordType,
      Consumer<T> listener) {
    final var recordListeners = listeners.getOrDefault(recordType, new ArrayList<>());
    recordListeners.add(listener);
    listeners.put(recordType, recordListeners);
  }

  private <T extends com.google.protobuf.Message> boolean handleRecord(
      Schema.Record genericRecord, Class<T> t) throws InvalidProtocolBufferException {

    if (genericRecord.getRecord().is(t)) {
      final var record = genericRecord.getRecord().unpack(t);

      listeners
          .getOrDefault(t, List.of())
          .forEach(listener -> ((Consumer<T>) listener).accept(record));

      return true;
    } else {
      return false;
    }
  }

  private <T> void ifEvent(
      final T record,
      final Function<T, Schema.RecordMetadata> extractor,
      final Consumer<T> consumer) {
    final var metadata = extractor.apply(record);
    if (isEvent(metadata)) {
      consumer.accept(record);
    }
  }

  private boolean isEvent(final Schema.RecordMetadata metadata) {
    return metadata.getRecordType() == Schema.RecordMetadata.RecordType.EVENT;
  }

  private Map<String, Object> parseProperties(final String propertiesString) {
    final Properties properties = new Properties();
    final Map<String, Object> parsed = new HashMap<>();

    try {
      properties.load(new StringReader(propertiesString));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    for (final String property : properties.stringPropertyNames()) {
      parsed.put(property, properties.get(property));
    }

    return parsed;
  }
}
