package io.zeebe.monitor.service;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.CatchEvent;
import io.camunda.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.FlowElement;
import io.camunda.zeebe.model.bpmn.instance.SequenceFlow;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.ErrorEntity;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.ProcessInstanceEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.ErrorRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.VariableRepository;
import io.zeebe.monitor.rest.ActiveScope;
import io.zeebe.monitor.rest.AuditLogEntry;
import io.zeebe.monitor.rest.BpmnElementInfo;
import io.zeebe.monitor.rest.CalledProcessInstanceDto;
import io.zeebe.monitor.rest.ElementInstanceState;
import io.zeebe.monitor.rest.ErrorDto;
import io.zeebe.monitor.rest.IncidentDto;
import io.zeebe.monitor.rest.IncidentListDto;
import io.zeebe.monitor.rest.JobDto;
import io.zeebe.monitor.rest.MessageDto;
import io.zeebe.monitor.rest.MessageSubscriptionDto;
import io.zeebe.monitor.rest.ProcessInstanceDto;
import io.zeebe.monitor.rest.TimerDto;
import io.zeebe.monitor.rest.VariableEntry;
import io.zeebe.monitor.rest.VariableUpdateEntry;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 应用模块名称<p> 代码描述<p> Copyright:  Copyright (C) 2021 tencent, Inc. All rights reserved. <p> Company:
 * tencent pcg<p>
 *
 * @author devinzhu
 * @since 2021/5/13 2:13 下午
 */
@Service
public class ProcessInstanceService {

  private static final List<String> PROCESS_INSTANCE_ENTERED_INTENTS =
      Arrays.asList("ELEMENT_ACTIVATED");
  private static final List<String> PROCESS_INSTANCE_COMPLETED_INTENTS =
      Arrays.asList("ELEMENT_COMPLETED", "ELEMENT_TERMINATED");
  private static final List<String> EXCLUDE_ELEMENT_TYPES =
      Arrays.asList(BpmnElementType.MULTI_INSTANCE_BODY.name());
  private static final List<String> JOB_COMPLETED_INTENTS = Arrays.asList("completed", "canceled");


  @Autowired
  private ProcessRepository processRepository;

  @Autowired
  private ProcessInstanceRepository processInstanceRepository;

  @Autowired
  private ElementInstanceRepository activityInstanceRepository;

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
  private VariableRepository variableRepository;
  @Autowired
  private ErrorRepository errorRepository;

  public ProcessInstanceDto findInstanceDetailByKeyNotExistThrowExp(long key) {
    final ProcessInstanceEntity instance =
        processInstanceRepository
            .findByKey(key)
            .orElseThrow(() -> new RuntimeException("No workflow instance found with key: " + key));
    return toInstanceDto(instance);
  }

  public ProcessInstanceDto findInstanceDetailByKey(long key) {
    final ProcessInstanceEntity instance =
        processInstanceRepository
            .findByKey(key).orElse(null);
    return instance == null ? null : toInstanceDto(instance);
  }

  public ProcessInstanceDto toInstanceDto(final ProcessInstanceEntity instance) {
    final List<ElementInstanceEntity> events =
        StreamSupport.stream(
            activityInstanceRepository
                .findByProcessInstanceKey(instance.getKey())
                .spliterator(),
            false)
            .collect(Collectors.toList());

    final ProcessInstanceDto dto = new ProcessInstanceDto();
    dto.setProcessInstanceKey(instance.getKey());

    dto.setPartitionId(instance.getPartitionId());

    dto.setProcessDefinitionKey(instance.getProcessDefinitionKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setVersion(instance.getVersion());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());
    dto.setRunning(!isEnded);

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    if (instance.getParentElementInstanceKey() > 0) {
      dto.setParentProcessInstanceKey(instance.getParentProcessInstanceKey());

      processInstanceRepository
          .findByKey(instance.getParentProcessInstanceKey())
          .ifPresent(
              parent -> {
                dto.setParentBpmnProcessId(parent.getBpmnProcessId());
              });
    }

    final List<String> completedActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());

    final List<String> activeActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .filter(id -> !completedActivities.contains(id))
            .collect(Collectors.toList());
    dto.setActiveActivities(activeActivities);

    final List<String> takenSequenceFlows =
        events.stream()
            .filter(e -> e.getIntent().equals("SEQUENCE_FLOW_TAKEN"))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());
    dto.setTakenSequenceFlows(takenSequenceFlows);

    final Map<String, Long> completedElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final Map<String, Long> enteredElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final List<ElementInstanceState> elementStates =
        enteredElementsById.entrySet().stream()
            .map(
                e -> {
                  final String elementId = e.getKey();

                  final long enteredInstances = e.getValue();
                  final long completedInstances = completedElementsById.getOrDefault(elementId, 0L);

                  final ElementInstanceState state = new ElementInstanceState();
                  state.setElementId(elementId);
                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());

    dto.setElementInstances(elementStates);

    final var bpmnModelInstance =
        processRepository
            .findByKey(instance.getProcessDefinitionKey())
            .map(w -> new ByteArrayInputStream(w.getResource().getBytes()))
            .map(Bpmn::readModelFromStream);

    final Map<String, String> flowElements = new HashMap<>();

    bpmnModelInstance.ifPresent(
        bpmn -> {
          bpmn.getModelElementsByType(FlowElement.class)
              .forEach(
                  e -> flowElements.put(e.getId(), Optional.ofNullable(e.getName()).orElse("")));

          dto.setBpmnElementInfos(getBpmnElementInfos(bpmn));
        });

    final List<AuditLogEntry> auditLogEntries =
        events.stream()
            .map(
                e -> {
                  final AuditLogEntry entry = new AuditLogEntry();

                  entry.setKey(e.getKey());
                  entry.setFlowScopeKey(e.getFlowScopeKey());
                  entry.setElementId(e.getElementId());
                  entry.setElementName(flowElements.getOrDefault(e.getElementId(), ""));
                  entry.setBpmnElementType(e.getBpmnElementType());
                  entry.setState(e.getIntent());
                  entry.setTimestamp(Instant.ofEpochMilli(e.getTimestamp()).toString());

                  return entry;
                })
            .collect(Collectors.toList());

    dto.setAuditLogEntries(auditLogEntries);

    final List<IncidentEntity> incidents =
        StreamSupport.stream(
            incidentRepository.findByProcessInstanceKey(instance.getKey()).spliterator(), false)
            .collect(Collectors.toList());

    final Map<Long, String> elementIdsForKeys = new HashMap<>();
    elementIdsForKeys.put(instance.getKey(), instance.getBpmnProcessId());
    events.forEach(e -> elementIdsForKeys.put(e.getKey(), e.getElementId()));

    final List<IncidentDto> incidentDtos =
        incidents.stream()
            .map(
                i -> {
                  final long incidentKey = i.getKey();

                  final IncidentDto incidentDto = new IncidentDto();
                  incidentDto.setKey(incidentKey);

                  incidentDto.setElementId(elementIdsForKeys.get(i.getElementInstanceKey()));
                  incidentDto.setElementInstanceKey(i.getElementInstanceKey());

                  if (i.getJobKey() > 0) {
                    incidentDto.setJobKey(i.getJobKey());
                  }

                  incidentDto.setErrorType(i.getErrorType());
                  incidentDto.setErrorMessage(i.getErrorMessage());

                  final boolean isResolved = i.getResolved() != null && i.getResolved() > 0;
                  incidentDto.setResolved(isResolved);

                  incidentDto.setCreatedTime(Instant.ofEpochMilli(i.getCreated()).toString());

                  if (isResolved) {
                    incidentDto.setResolvedTime(Instant.ofEpochMilli(i.getResolved()).toString());

                    incidentDto.setState("Resolved");
                  } else {
                    incidentDto.setState("Created");
                  }

                  return incidentDto;
                })
            .collect(Collectors.toList());
    dto.setIncidents(incidentDtos);

    final List<String> activitiesWitIncidents =
        incidents.stream()
            .filter(i -> i.getResolved() == null || i.getResolved() <= 0)
            .map(i -> elementIdsForKeys.get(i.getElementInstanceKey()))
            .distinct()
            .collect(Collectors.toList());

    dto.setIncidentActivities(activitiesWitIncidents);

    activeActivities.removeAll(activitiesWitIncidents);
    dto.setActiveActivities(activeActivities);

    final Map<VariableTuple, List<VariableEntity>> variablesByScopeAndName =
        variableRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .collect(Collectors.groupingBy(v -> new VariableTuple(v.getScopeKey(), v.getName())));
    variablesByScopeAndName.forEach(
        (scopeKeyName, variables) -> {
          final VariableEntry variableDto = new VariableEntry();
          final long scopeKey = scopeKeyName.scopeKey;

          variableDto.setScopeKey(scopeKey);
          variableDto.setElementId(elementIdsForKeys.get(scopeKey));

          variableDto.setName(scopeKeyName.name);

          final VariableEntity lastUpdate = variables.get(variables.size() - 1);
          variableDto.setValue(lastUpdate.getValue());
          variableDto.setTimestamp(Instant.ofEpochMilli(lastUpdate.getTimestamp()).toString());

          final List<VariableUpdateEntry> varUpdates =
              variables.stream()
                  .map(
                      v -> {
                        final VariableUpdateEntry varUpdate = new VariableUpdateEntry();
                        varUpdate.setValue(v.getValue());
                        varUpdate.setTimestamp(Instant.ofEpochMilli(v.getTimestamp()).toString());
                        return varUpdate;
                      })
                  .collect(Collectors.toList());
          variableDto.setUpdates(varUpdates);

          dto.getVariables().add(variableDto);
        });

    final List<ActiveScope> activeScopes = new ArrayList<>();
    if (!isEnded) {
      activeScopes.add(new ActiveScope(instance.getKey(), instance.getBpmnProcessId()));

      final List<Long> completedElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .collect(Collectors.toList());

      final List<ActiveScope> activeElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .filter(id -> !completedElementInstances.contains(id))
              .map(scopeKey -> new ActiveScope(scopeKey, elementIdsForKeys.get(scopeKey)))
              .collect(Collectors.toList());

      activeScopes.addAll(activeElementInstances);
    }
    dto.setActiveScopes(activeScopes);

    final List<JobDto> jobDtos =
        jobRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(
                job -> {
                  final JobDto jobDto = toDto(job);
                  jobDto.setElementId(
                      elementIdsForKeys.getOrDefault(job.getElementInstanceKey(), ""));

                  final boolean isActivatable =
                      job.getRetries() > 0
                          && Arrays.asList("created", "failed", "timed_out", "retries_updated")
                          .contains(job.getState());
                  jobDto.setActivatable(isActivatable);

                  return jobDto;
                })
            .collect(Collectors.toList());
    dto.setJobs(jobDtos);

    final List<MessageSubscriptionDto> messageSubscriptions =
        messageSubscriptionRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(
                subscription -> {
                  final MessageSubscriptionDto subscriptionDto = toDto(subscription);
                  subscriptionDto.setElementId(
                      elementIdsForKeys.getOrDefault(subscriptionDto.getElementInstanceKey(), ""));

                  return subscriptionDto;
                })
            .collect(Collectors.toList());
    dto.setMessageSubscriptions(messageSubscriptions);

    final List<TimerDto> timers =
        timerRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    dto.setTimers(timers);

    final var calledProcessInstances =
        processInstanceRepository.findByParentProcessInstanceKey(instance.getKey()).stream()
            .map(
                childEntity -> {
                  final var childDto = new CalledProcessInstanceDto();

                  childDto.setChildProcessInstanceKey(childEntity.getKey());
                  childDto.setChildBpmnProcessId(childEntity.getBpmnProcessId());
                  childDto.setChildState(childEntity.getState());

                  childDto.setElementInstanceKey(childEntity.getParentElementInstanceKey());

                  final var callElementId =
                      elementIdsForKeys.getOrDefault(childEntity.getParentElementInstanceKey(), "");
                  childDto.setElementId(callElementId);

                  return childDto;
                })
            .collect(Collectors.toList());
    dto.setCalledProcessInstances(calledProcessInstances);

    final var errors =
        errorRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    dto.setErrors(errors);

    return dto;
  }

  private IncidentListDto toDto(final IncidentEntity incident) {
    final IncidentListDto dto = new IncidentListDto();
    dto.setKey(incident.getKey());

    dto.setBpmnProcessId(incident.getBpmnProcessId());
    dto.setProcessDefinitionKey(incident.getProcessDefinitionKey());
    dto.setProcessInstanceKey(incident.getProcessInstanceKey());

    dto.setErrorType(incident.getErrorType());
    dto.setErrorMessage(incident.getErrorMessage());

    final boolean isResolved = incident.getResolved() != null && incident.getResolved() > 0;

    dto.setCreatedTime(Instant.ofEpochMilli(incident.getCreated()).toString());

    if (isResolved) {
      dto.setResolvedTime(Instant.ofEpochMilli(incident.getResolved()).toString());

      dto.setState("Resolved");
    } else {
      dto.setState("Created");
    }

    return dto;
  }

  private JobDto toDto(final JobEntity job) {
    final JobDto dto = new JobDto();

    dto.setKey(job.getKey());
    dto.setJobType(job.getJobType());
    dto.setProcessInstanceKey(job.getProcessInstanceKey());
    dto.setElementInstanceKey(job.getElementInstanceKey());
    dto.setState(job.getState());
    dto.setRetries(job.getRetries());
    Optional.ofNullable(job.getWorker()).ifPresent(dto::setWorker);
    dto.setTimestamp(Instant.ofEpochMilli(job.getTimestamp()).toString());

    return dto;
  }

  private MessageDto toDto(final MessageEntity message) {
    final MessageDto dto = new MessageDto();

    dto.setKey(message.getKey());
    dto.setName(message.getName());
    dto.setCorrelationKey(message.getCorrelationKey());
    dto.setMessageId(message.getMessageId());
    dto.setPayload(message.getPayload());
    dto.setState(message.getState());
    dto.setTimestamp(Instant.ofEpochMilli(message.getTimestamp()).toString());

    return dto;
  }

  private MessageSubscriptionDto toDto(final MessageSubscriptionEntity subscription) {
    final MessageSubscriptionDto dto = new MessageSubscriptionDto();

    dto.setKey(subscription.getId());
    dto.setMessageName(subscription.getMessageName());
    dto.setCorrelationKey(Optional.ofNullable(subscription.getCorrelationKey()).orElse(""));

    dto.setProcessInstanceKey(subscription.getProcessInstanceKey());
    dto.setElementInstanceKey(subscription.getElementInstanceKey());

    dto.setElementId(subscription.getTargetFlowNodeId());

    dto.setState(subscription.getState());
    dto.setTimestamp(Instant.ofEpochMilli(subscription.getTimestamp()).toString());

    dto.setOpen(subscription.getState().equalsIgnoreCase(MessageSubscriptionIntent.CREATED.name()));

    return dto;
  }

  private TimerDto toDto(final TimerEntity timer) {
    final TimerDto dto = new TimerDto();

    dto.setElementId(timer.getTargetElementId());
    dto.setState(timer.getState());
    dto.setDueDate(Instant.ofEpochMilli(timer.getDueDate()).toString());
    dto.setTimestamp(Instant.ofEpochMilli(timer.getTimestamp()).toString());
    dto.setElementInstanceKey(timer.getElementInstanceKey());

    final int repetitions = timer.getRepetitions();
    dto.setRepetitions(repetitions >= 0 ? String.valueOf(repetitions) : "∞");

    return dto;
  }

  private ErrorDto toDto(final ErrorEntity entity) {
    final var dto = new ErrorDto();
    dto.setPosition(entity.getPosition());
    dto.setErrorEventPosition(entity.getErrorEventPosition());
    dto.setExceptionMessage(entity.getExceptionMessage());
    dto.setStacktrace(entity.getStacktrace());
    dto.setTimestamp(Instant.ofEpochMilli(entity.getTimestamp()).toString());

    if (entity.getProcessInstanceKey() > 0) {
      dto.setProcessInstanceKey(entity.getProcessInstanceKey());
    }

    return dto;
  }


  private List<BpmnElementInfo> getBpmnElementInfos(final BpmnModelInstance bpmn) {
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

  private String formatMs(long start) {
    return Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }


  private static class VariableTuple {

    private final long scopeKey;
    private final String name;

    VariableTuple(final long scopeKey, final String name) {
      this.scopeKey = scopeKey;
      this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final VariableTuple that = (VariableTuple) o;
      return scopeKey == that.scopeKey && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scopeKey, name);
    }
  }
}
