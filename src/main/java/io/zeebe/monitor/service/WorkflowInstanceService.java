package io.zeebe.monitor.service;

import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.instance.CatchEvent;
import io.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.entity.WorkflowInstanceEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.VariableRepository;
import io.zeebe.monitor.repository.WorkflowInstanceRepository;
import io.zeebe.monitor.repository.WorkflowRepository;
import io.zeebe.monitor.rest.ActiveScope;
import io.zeebe.monitor.rest.AuditLogEntry;
import io.zeebe.monitor.rest.BpmnElementInfo;
import io.zeebe.monitor.rest.CalledWorkflowInstanceDto;
import io.zeebe.monitor.rest.ElementInstanceState;
import io.zeebe.monitor.rest.IncidentDto;
import io.zeebe.monitor.rest.IncidentListDto;
import io.zeebe.monitor.rest.JobDto;
import io.zeebe.monitor.rest.MessageDto;
import io.zeebe.monitor.rest.MessageSubscriptionDto;
import io.zeebe.monitor.rest.TimerDto;
import io.zeebe.monitor.rest.VariableEntry;
import io.zeebe.monitor.rest.VariableUpdateEntry;
import io.zeebe.monitor.rest.WorkflowInstanceDto;
import io.zeebe.protocol.record.value.BpmnElementType;
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
public class WorkflowInstanceService {

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

  public WorkflowInstanceDto findInstanceDetailByKey(long key) {
    final WorkflowInstanceEntity instance =
        workflowInstanceRepository
            .findByKey(key)
            .orElseThrow(() -> new RuntimeException("No workflow instance found with key: " + key));
    return toInstanceDto(instance);
  }

  private WorkflowInstanceDto toInstanceDto(WorkflowInstanceEntity instance) {
    final List<ElementInstanceEntity> events =
        StreamSupport.stream(
            activityInstanceRepository
                .findByWorkflowInstanceKey(instance.getKey())
                .spliterator(),
            false)
            .collect(Collectors.toList());

    final WorkflowInstanceDto dto = new WorkflowInstanceDto();
    dto.setWorkflowInstanceKey(instance.getKey());

    dto.setPartitionId(instance.getPartitionId());

    dto.setWorkflowKey(instance.getWorkflowKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setVersion(instance.getVersion());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());
    dto.setRunning(!isEnded);

    dto.setStartTime(formatMs(instance.getStart()));

    if (isEnded) {
      dto.setEndTime(formatMs(instance.getEnd()));
    }

    if (instance.getParentElementInstanceKey() > 0) {
      dto.setParentWorkflowInstanceKey(instance.getParentWorkflowInstanceKey());

      workflowInstanceRepository
          .findByKey(instance.getParentWorkflowInstanceKey())
          .ifPresent(
              parent -> {
                dto.setParentBpmnProcessId(parent.getBpmnProcessId());
              });
    }

    final List<String> completedActivities =
        events.stream()
            .filter(e -> WORKFLOW_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> e.getBpmnElementType() != BpmnElementType.PROCESS.name())
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());

    final List<String> activeActivities =
        events.stream()
            .filter(e -> WORKFLOW_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> e.getBpmnElementType() != BpmnElementType.PROCESS.name())
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
            .filter(e -> WORKFLOW_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final Map<String, Long> enteredElementsById =
        events.stream()
            .filter(e -> WORKFLOW_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
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
        workflowRepository
            .findByKey(instance.getWorkflowKey())
            .map(w -> new ByteArrayInputStream(w.getResource().getBytes()))
            .map(stream -> Bpmn.readModelFromStream(stream));

    final Map<String, String> flowElements = new HashMap<>();

    bpmnModelInstance.ifPresent(
        bpmn -> {
          bpmn.getModelElementsByType(FlowElement.class)
              .forEach(
                  e -> {
                    flowElements.put(e.getId(), Optional.ofNullable(e.getName()).orElse(""));
                  });

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
                  entry.setTimestamp(formatMs(e.getTimestamp()));

                  return entry;
                })
            .collect(Collectors.toList());

    dto.setAuditLogEntries(auditLogEntries);

    final List<IncidentEntity> incidents =
        StreamSupport.stream(
            incidentRepository.findByWorkflowInstanceKey(instance.getKey()).spliterator(),
            false)
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

                  incidentDto.setCreatedTime(formatMs(i.getCreated()));

                  if (isResolved) {
                    incidentDto.setResolvedTime(formatMs(i.getResolved()));

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
        variableRepository.findByWorkflowInstanceKey(instance.getKey()).stream()
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
          variableDto.setTimestamp(formatMs(lastUpdate.getTimestamp()));

          final List<VariableUpdateEntry> varUpdates =
              variables.stream()
                  .map(
                      v -> {
                        final VariableUpdateEntry varUpdate = new VariableUpdateEntry();
                        varUpdate.setValue(v.getValue());
                        varUpdate.setTimestamp(formatMs(v.getTimestamp()));
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
              .filter(e -> WORKFLOW_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .collect(Collectors.toList());

      final List<ActiveScope> activeElementInstances =
          events.stream()
              .filter(e -> WORKFLOW_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .filter(id -> !completedElementInstances.contains(id))
              .map(scopeKey -> new ActiveScope(scopeKey, elementIdsForKeys.get(scopeKey)))
              .collect(Collectors.toList());

      activeScopes.addAll(activeElementInstances);
    }
    dto.setActiveScopes(activeScopes);

    final List<JobDto> jobDtos =
        jobRepository.findByWorkflowInstanceKey(instance.getKey()).stream()
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
        messageSubscriptionRepository.findByWorkflowInstanceKey(instance.getKey()).stream()
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
        timerRepository.findByWorkflowInstanceKey(instance.getKey()).stream()
            .map(timer -> toDto(timer))
            .collect(Collectors.toList());
    dto.setTimers(timers);

    final var calledWorkflowInstances =
        workflowInstanceRepository.findByParentWorkflowInstanceKey(instance.getKey()).stream()
            .map(
                childEntity -> {
                  final var childDto = new CalledWorkflowInstanceDto();

                  childDto.setChildWorkflowInstanceKey(childEntity.getKey());
                  childDto.setChildBpmnProcessId(childEntity.getBpmnProcessId());
                  childDto.setChildState(childEntity.getState());

                  childDto.setElementInstanceKey(childEntity.getParentElementInstanceKey());

                  final var callElementId =
                      elementIdsForKeys.getOrDefault(childEntity.getParentElementInstanceKey(), "");
                  childDto.setElementId(callElementId);

                  return childDto;
                })
            .collect(Collectors.toList());
    dto.setCalledWorkflowInstances(calledWorkflowInstances);

    return dto;
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

  private String formatMs(long start) {
    return Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  private static class VariableTuple {

    private final long scopeKey;
    private final String name;

    VariableTuple(long scopeKey, String name) {
      this.scopeKey = scopeKey;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      VariableTuple that = (VariableTuple) o;
      return scopeKey == that.scopeKey && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scopeKey, name);
    }
  }

}
