/*
 * Copyright 2021 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.jflyte;

import static org.flyte.jflyte.MoreCollectors.toUnmodifiableList;

import java.util.List;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;

class WorkflowNodeVisitor {

  WorkflowTemplate visitWorkflowTemplate(WorkflowTemplate template) {
    List<Node> newNodes =
        template.nodes().stream().map(this::visitNode).collect(toUnmodifiableList());

    return template.toBuilder().nodes(newNodes).build();
  }

  Node visitNode(Node node) {
    return node.toBuilder()
        .branchNode(node.branchNode() != null ? visitBranchNode(node.branchNode()) : null)
        .taskNode(node.taskNode() != null ? visitTaskNode(node.taskNode()) : null)
        .workflowNode(node.workflowNode() != null ? visitWorkflowNode(node.workflowNode()) : null)
        .build();
  }

  TaskNode visitTaskNode(TaskNode taskNode) {
    return TaskNode.builder().referenceId(visitTaskIdentifier(taskNode.referenceId())).build();
  }

  WorkflowNode visitWorkflowNode(WorkflowNode workflowNode) {
    return workflowNode.toBuilder()
        .reference(visitWorkflowNodeReference(workflowNode.reference()))
        .build();
  }

  BranchNode visitBranchNode(BranchNode branchNode) {
    return branchNode.toBuilder().ifElse(visitIfElseBlock(branchNode.ifElse())).build();
  }

  IfElseBlock visitIfElseBlock(IfElseBlock ifElse) {
    return ifElse.toBuilder()
        .case_(visitIfBlock(ifElse.case_()))
        .other(ifElse.other().stream().map(this::visitIfBlock).collect(toUnmodifiableList()))
        .elseNode(ifElse.elseNode() != null ? visitNode(ifElse.elseNode()) : null)
        .build();
  }

  IfBlock visitIfBlock(IfBlock ifBlock) {
    return ifBlock.toBuilder().thenNode(visitNode(ifBlock.thenNode())).build();
  }

  WorkflowNode.Reference visitWorkflowNodeReference(WorkflowNode.Reference reference) {
    switch (reference.kind()) {
      case LAUNCH_PLAN_REF:
        return WorkflowNode.Reference.ofLaunchPlanRef(
            visitLaunchPlanIdentifier(reference.launchPlanRef()));
      case SUB_WORKFLOW_REF:
        return WorkflowNode.Reference.ofSubWorkflowRef(
            visitWorkflowIdentifier(reference.subWorkflowRef()));
    }

    throw new AssertionError("Unexpected WorkflowNode.Reference.Kind: " + reference.kind());
  }

  PartialTaskIdentifier visitTaskIdentifier(PartialTaskIdentifier value) {
    return value;
  }

  PartialWorkflowIdentifier visitWorkflowIdentifier(PartialWorkflowIdentifier value) {
    return value;
  }

  PartialLaunchPlanIdentifier visitLaunchPlanIdentifier(PartialLaunchPlanIdentifier value) {
    return value;
  }
}
