package io.zeebe.monitor.rest;

import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.instance.CatchEvent;
import io.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.zeebe.monitor.entity.ElementInstanceStatistics;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.entity.WorkflowEntity;
import io.zeebe.monitor.entity.WorkflowInstanceEntity;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.WorkflowInstanceRepository;
import io.zeebe.monitor.repository.WorkflowRepository;
import io.zeebe.monitor.service.WorkflowInstanceService;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.transaction.Transactional;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@Controller
public class ViewController {

  private static final List<String> WORKFLOW_INSTANCE_ENTERED_INTENTS =
      Arrays.asList("ELEMENT_ACTIVATED");

  private static final List<String> WORKFLOW_INSTANCE_COMPLETED_INTENTS =
      Arrays.asList("ELEMENT_COMPLETED", "ELEMENT_TERMINATED");

  private static final List<String> EXCLUDE_ELEMENT_TYPES =
      Arrays.asList(BpmnElementType.MULTI_INSTANCE_BODY.name());

  private static final List<String> JOB_COMPLETED_INTENTS = Arrays.asList("completed", "canceled");

  @Autowired
  private WorkflowRepository workflowRepository;

  @Autowired
  private WorkflowInstanceRepository workflowInstanceRepository;

  @Autowired
  private IncidentRepository incidentRepository;

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private MessageRepository messageRepository;

  @Autowired
  private MessageSubscriptionRepository messageSubscriptionRepository;

  @Autowired
  private TimerRepository timerRepository;

  @Autowired
  private WorkflowInstanceService workflowInstanceService;

  private final String base_path;

  public ViewController(@Value("${server.servlet.context-path}") final String base_path) {
    this.base_path = base_path.endsWith("/") ? base_path : base_path + "/";
  }

  @GetMapping("/")
  public String index(Map<String, Object> model, Pageable pageable) {
    addContextPathToModel(model);
    return workflowList(model, pageable);
  }

  @GetMapping("/views/workflows")
  public String workflowList(Map<String, Object> model, Pageable pageable) {

    final long count = workflowRepository.count();

    final List<WorkflowDto> workflows = new ArrayList<>();
    for (WorkflowEntity workflowEntity : workflowRepository.findAll(pageable)) {
      final WorkflowDto dto = toDto(workflowEntity);
      workflows.add(dto);
    }

    model.put("workflows", workflows);
    model.put("count", count);

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "workflow-list-view";
  }

  private WorkflowDto toDto(WorkflowEntity workflowEntity) {
    final long workflowKey = workflowEntity.getKey();

    final long running = workflowInstanceRepository.countByWorkflowKeyAndEndIsNull(workflowKey);
    final long ended = workflowInstanceRepository.countByWorkflowKeyAndEndIsNotNull(workflowKey);

    final WorkflowDto dto = WorkflowDto.from(workflowEntity, running, ended);
    return dto;
  }

  @GetMapping("/views/workflows/{key}")
  @Transactional
  public String workflowDetail(
      @PathVariable long key, Map<String, Object> model, Pageable pageable) {

    final WorkflowEntity workflow =
        workflowRepository
            .findByKey(key)
            .orElseThrow(() -> new RuntimeException("No workflow found with key: " + key));

    model.put("workflow", toDto(workflow));
    model.put("resource", workflow.getResource());

    final List<ElementInstanceState> elementInstanceStates = getElementInstanceStates(key);
    model.put("instance.elementInstances", elementInstanceStates);

    final long count = workflowInstanceRepository.countByWorkflowKey(key);

    final List<WorkflowInstanceListDto> instances = new ArrayList<>();
    for (WorkflowInstanceEntity instanceEntity :
        workflowInstanceRepository.findByWorkflowKey(key, pageable)) {
      instances.add(toDto(instanceEntity));
    }

    model.put("instances", instances);
    model.put("count", count);

    final List<TimerDto> timers =
        timerRepository.findByWorkflowKeyAndWorkflowInstanceKeyIsNull(key).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    model.put("timers", timers);

    final List<MessageSubscriptionDto> messageSubscriptions =
        messageSubscriptionRepository.findByWorkflowKeyAndWorkflowInstanceKeyIsNull(key).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    model.put("messageSubscriptions", messageSubscriptions);

    final var resourceAsStream = new ByteArrayInputStream(workflow.getResource().getBytes());
    final var bpmn = Bpmn.readModelFromStream(resourceAsStream);
    model.put("instance.bpmnElementInfos", getBpmnElementInfos(bpmn));

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "workflow-detail-view";
  }

  private List<ElementInstanceState> getElementInstanceStates(long key) {

    final List<ElementInstanceStatistics> elementEnteredStatistics =
        workflowRepository.getElementInstanceStatisticsByKeyAndIntentIn(
            key, WORKFLOW_INSTANCE_ENTERED_INTENTS, EXCLUDE_ELEMENT_TYPES);

    final Map<String, Long> elementCompletedCount =
        workflowRepository
            .getElementInstanceStatisticsByKeyAndIntentIn(
                key, WORKFLOW_INSTANCE_COMPLETED_INTENTS, EXCLUDE_ELEMENT_TYPES)
            .stream()
            .collect(
                Collectors.toMap(
                    ElementInstanceStatistics::getElementId, ElementInstanceStatistics::getCount));

    final List<ElementInstanceState> elementInstanceStates =
        elementEnteredStatistics.stream()
            .map(
                s -> {
                  final ElementInstanceState state = new ElementInstanceState();

                  final String elementId = s.getElementId();
                  state.setElementId(elementId);

                  final long completedInstances = elementCompletedCount.getOrDefault(elementId, 0L);
                  long enteredInstances = s.getCount();

                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());
    return elementInstanceStates;
  }

  private WorkflowInstanceListDto toDto(WorkflowInstanceEntity instance) {

    final WorkflowInstanceListDto dto = new WorkflowInstanceListDto();
    dto.setWorkflowInstanceKey(instance.getKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setWorkflowKey(instance.getWorkflowKey());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());

    dto.setStartTime(formatMs(instance.getStart()));

    if (isEnded) {
      dto.setEndTime(formatMs(instance.getEnd()));
    }

    return dto;
  }

  private String formatMs(long start) {
    return Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  @GetMapping("/views/instances")
  public String instanceList(Map<String, Object> model, Pageable pageable) {

    final long count = workflowInstanceRepository.count();

    final List<WorkflowInstanceListDto> instances = new ArrayList<>();
    for (WorkflowInstanceEntity instanceEntity : workflowInstanceRepository.findAll(pageable)) {
      final WorkflowInstanceListDto dto = toDto(instanceEntity);
      instances.add(dto);
    }

    model.put("instances", instances);
    model.put("count", count);

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "instance-list-view";
  }

  @GetMapping("/views/instances/{key}")
  @Transactional
  public String instanceDetail(
      @PathVariable long key, Map<String, Object> model, Pageable pageable) {
    WorkflowInstanceDto instanceDto = workflowInstanceService
        .findInstanceDetailByKeyNotExistThrowExp(key);

    workflowRepository
        .findByKey(instanceDto.getWorkflowKey())
        .ifPresent(workflow -> model.put("resource", workflow.getResource()));

    model.put("instance", instanceDto);

    addContextPathToModel(model);

    return "instance-detail-view";
  }

  private List<BpmnElementInfo> getBpmnElementInfos(BpmnModelInstance bpmn) {
    final List<BpmnElementInfo> infos = new ArrayList<>();

    bpmn.getModelElementsByType(ServiceTask.class)
        .forEach(
            t -> {
              final var info = new BpmnElementInfo();
              info.setElementId(t.getId());
              final var jobType = t.getSingleExtensionElement(ZeebeTaskDefinition.class).getType();
              info.setInfo("job-type: " + jobType);

              infos.add(info);
            });

    bpmn.getModelElementsByType(SequenceFlow.class)
        .forEach(
            s -> {
              final var conditionExpression = s.getConditionExpression();

              if (conditionExpression != null && !conditionExpression.getTextContent().isEmpty()) {

                final var info = new BpmnElementInfo();
                info.setElementId(s.getId());
                final var condition = conditionExpression.getTextContent();
                info.setInfo("condition: " + condition);

                infos.add(info);
              }
            });

    bpmn.getModelElementsByType(CatchEvent.class)
        .forEach(
            catchEvent -> {
              final var info = new BpmnElementInfo();
              info.setElementId(catchEvent.getId());

              catchEvent
                  .getEventDefinitions()
                  .forEach(
                      eventDefinition -> {
                        if (eventDefinition instanceof ErrorEventDefinition) {
                          final var errorEventDefinition = (ErrorEventDefinition) eventDefinition;
                          final var errorCode = errorEventDefinition.getError().getErrorCode();

                          info.setInfo("errorCode: " + errorCode);
                          infos.add(info);
                        }

                        if (eventDefinition instanceof TimerEventDefinition) {
                          final var timerEventDefinition = (TimerEventDefinition) eventDefinition;

                          Optional.<ModelElementInstance>ofNullable(
                              timerEventDefinition.getTimeCycle())
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDate()))
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDuration()))
                              .map(ModelElementInstance::getTextContent)
                              .ifPresent(
                                  timer -> {
                                    info.setInfo("timer: " + timer);
                                    infos.add(info);
                                  });
                        }
                      });
            });

    return infos;
  }

  @GetMapping("/views/incidents")
  @Transactional
  public String incidentList(Map<String, Object> model, Pageable pageable) {

    final long count = incidentRepository.countByResolvedIsNull();

    final List<IncidentListDto> incidents = new ArrayList<>();
    for (IncidentEntity incidentEntity : incidentRepository.findByResolvedIsNull(pageable)) {
      final IncidentListDto dto = toDto(incidentEntity);
      incidents.add(dto);
    }

    model.put("incidents", incidents);
    model.put("count", count);

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "incident-list-view";
  }

  private IncidentListDto toDto(IncidentEntity incident) {
    final IncidentListDto dto = new IncidentListDto();
    dto.setKey(incident.getKey());

    dto.setBpmnProcessId(incident.getBpmnProcessId());
    dto.setWorkflowKey(incident.getWorkflowKey());
    ;
    dto.setWorkflowInstanceKey(incident.getWorkflowInstanceKey());

    dto.setErrorType(incident.getErrorType());
    dto.setErrorMessage(incident.getErrorMessage());

    final boolean isResolved = incident.getResolved() != null && incident.getResolved() > 0;

    dto.setCreatedTime(formatMs(incident.getCreated()));

    if (isResolved) {
      dto.setResolvedTime(formatMs(incident.getResolved()));

      dto.setState("Resolved");
    } else {
      dto.setState("Created");
    }

    return dto;
  }

  @GetMapping("/views/jobs")
  public String jobList(Map<String, Object> model, Pageable pageable) {

    final long count = jobRepository.countByStateNotIn(JOB_COMPLETED_INTENTS);

    final List<JobDto> dtos = new ArrayList<>();
    for (JobEntity jobEntity : jobRepository.findByStateNotIn(JOB_COMPLETED_INTENTS, pageable)) {
      final JobDto dto = toDto(jobEntity);
      dtos.add(dto);
    }

    model.put("jobs", dtos);
    model.put("count", count);

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "job-list-view";
  }

  private JobDto toDto(JobEntity job) {
    final JobDto dto = new JobDto();

    dto.setKey(job.getKey());
    dto.setJobType(job.getJobType());
    dto.setWorkflowInstanceKey(job.getWorkflowInstanceKey());
    dto.setElementInstanceKey(job.getElementInstanceKey());
    dto.setState(job.getState());
    dto.setRetries(job.getRetries());
    Optional.ofNullable(job.getWorker()).ifPresent(dto::setWorker);
    dto.setTimestamp(formatMs(job.getTimestamp()));

    return dto;
  }

  @GetMapping("/views/messages")
  public String messageList(Map<String, Object> model, Pageable pageable) {

    final long count = messageRepository.count();

    final List<MessageDto> dtos = new ArrayList<>();
    for (MessageEntity messageEntity : messageRepository.findAll(pageable)) {
      final MessageDto dto = toDto(messageEntity);
      dtos.add(dto);
    }

    model.put("messages", dtos);
    model.put("count", count);

    addContextPathToModel(model);
    addPaginationToModel(model, pageable, count);

    return "message-list-view";
  }

  private MessageDto toDto(MessageEntity message) {
    final MessageDto dto = new MessageDto();

    dto.setKey(message.getKey());
    dto.setName(message.getName());
    dto.setCorrelationKey(message.getCorrelationKey());
    dto.setMessageId(message.getMessageId());
    dto.setPayload(message.getPayload());
    dto.setState(message.getState());
    dto.setTimestamp(formatMs(message.getTimestamp()));

    return dto;
  }

  private MessageSubscriptionDto toDto(MessageSubscriptionEntity subscription) {
    final MessageSubscriptionDto dto = new MessageSubscriptionDto();

    dto.setKey(subscription.getId());
    dto.setMessageName(subscription.getMessageName());
    dto.setCorrelationKey(Optional.ofNullable(subscription.getCorrelationKey()).orElse(""));

    dto.setWorkflowInstanceKey(subscription.getWorkflowInstanceKey());
    dto.setElementInstanceKey(subscription.getElementInstanceKey());

    dto.setElementId(subscription.getTargetFlowNodeId());

    dto.setState(subscription.getState());
    dto.setTimestamp(formatMs(subscription.getTimestamp()));

    dto.setOpen(subscription.getState().equals("opened"));

    return dto;
  }

  private TimerDto toDto(TimerEntity timer) {
    final TimerDto dto = new TimerDto();

    dto.setElementId(timer.getTargetFlowNodeId());
    dto.setState(timer.getState());
    dto.setDueDate(formatMs(timer.getDueDate()));
    dto.setTimestamp(formatMs(timer.getTimestamp()));
    dto.setElementInstanceKey(timer.getElementInstanceKey());

    final int repetitions = timer.getRepetitions();
    dto.setRepetitions(repetitions >= 0 ? String.valueOf(repetitions) : "∞");

    return dto;
  }

  private void addPaginationToModel(
      Map<String, Object> model, Pageable pageable, final long count) {

    final int currentPage = pageable.getPageNumber();
    model.put("page", currentPage + 1);
    if (currentPage > 0) {
      model.put("prevPage", currentPage - 1);
    }
    if (count > (1 + currentPage) * pageable.getPageSize()) {
      model.put("nextPage", currentPage + 1);
    }
  }

  private void addContextPathToModel(Map<String, Object> model) {
    model.put("context-path", base_path);
  }
}
