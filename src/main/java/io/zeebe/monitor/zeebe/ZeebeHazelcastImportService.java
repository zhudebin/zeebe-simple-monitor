package io.zeebe.monitor.zeebe;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.hazelcast.connect.java.ZeebeHazelcast;
import io.zeebe.monitor.entity.HazelcastConfig;
import io.zeebe.monitor.repository.HazelcastConfigRepository;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "zeebe.exporter", havingValue = "hazelcast")
public class ZeebeHazelcastImportService {

  private static final Logger LOG = LoggerFactory.getLogger(ZeebeHazelcastImportService.class);

  @Value("${zeebe.client.worker.hazelcast.connection}")
  private String hazelcastConnection;

  @Value("${zeebe.client.worker.hazelcast.connectionTimeout}")
  private String hazelcastConnectionTimeout;

  @Autowired
  private HazelcastConfigRepository hazelcastConfigRepository;

  @Autowired
  private ImportService importService;

  private AutoCloseable closeable;

  @PostConstruct
  public void start() {
    final ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig().addAddress(hazelcastConnection);

    final var connectionRetryConfig =
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig();
    connectionRetryConfig.setClusterConnectTimeoutMillis(
        Duration.parse(hazelcastConnectionTimeout).toMillis());

    LOG.info("Connecting to Hazelcast '{}'", hazelcastConnection);

    final HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient(clientConfig);

    LOG.info("Importing records from Hazelcast...");
    closeable = this.importFrom(hazelcast);
  }

  @PreDestroy
  public void close() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }

  public ZeebeHazelcast importFrom(final HazelcastInstance hazelcast) {

    final var hazelcastConfig =
        hazelcastConfigRepository
            .findById("cfg")
            .orElseGet(
                () -> {
                  final var config = new HazelcastConfig();
                  config.setId("cfg");
                  config.setSequence(-1);
                  return config;
                });

    final var builder =
        ZeebeHazelcast.newBuilder(hazelcast)
            .addProcessListener(
                record -> ifEvent(record, Schema.ProcessRecord::getMetadata,
                    importService::importProcess))
            .addProcessInstanceListener(
                record ->
                    ifEvent(
                        record,
                        Schema.ProcessInstanceRecord::getMetadata,
                        importService::importProcessInstance))
            .addIncidentListener(
                record -> ifEvent(record, Schema.IncidentRecord::getMetadata,
                    importService::importIncident))
            .addJobListener(
                record -> ifEvent(record, Schema.JobRecord::getMetadata, importService::importJob))
            .addVariableListener(
                record -> ifEvent(record, Schema.VariableRecord::getMetadata,
                    importService::importVariable))
            .addTimerListener(
                record -> ifEvent(record, Schema.TimerRecord::getMetadata,
                    importService::importTimer))
            .addMessageListener(
                record -> ifEvent(record, Schema.MessageRecord::getMetadata,
                    importService::importMessage))
            .addMessageSubscriptionListener(
                record ->
                    ifEvent(
                        record,
                        Schema.MessageSubscriptionRecord::getMetadata,
                        importService::importMessageSubscription))
            .addMessageStartEventSubscriptionListener(
                record ->
                    ifEvent(
                        record,
                        Schema.MessageStartEventSubscriptionRecord::getMetadata,
                        importService::importMessageStartEventSubscription))
            .addErrorListener(importService::importError)
            .postProcessListener(
                sequence -> {
                  hazelcastConfig.setSequence(sequence);
                  hazelcastConfigRepository.save(hazelcastConfig);
                });

    if (hazelcastConfig.getSequence() >= 0) {
      builder.readFrom(hazelcastConfig.getSequence());
    } else {
      builder.readFromHead();
    }

    return builder.build();
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
}
