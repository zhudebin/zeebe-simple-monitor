package io.zeebe.monitor.rest;

import io.zeebe.monitor.service.WorkflowInstanceService;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api")
public class ApiController {

  @Autowired
  private WorkflowInstanceService workflowInstanceService;

  @GetMapping("/instances/{key}")
  @ResponseBody
  @Transactional
  public WorkflowInstanceDto instanceDetail(@PathVariable long key) {
    return workflowInstanceService.findInstanceDetailByKey(key);
  }
}
