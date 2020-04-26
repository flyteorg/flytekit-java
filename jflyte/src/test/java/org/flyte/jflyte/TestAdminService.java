/*
 * Copyright 2020 Spotify AB.
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

import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.TaskOuterClass;
import flyteidl.admin.WorkflowOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.stub.StreamObserver;

/** Test implementation of Admin Service. This is needed as stubs are not mockable */
public class TestAdminService extends AdminServiceGrpc.AdminServiceImplBase {

  TaskOuterClass.TaskCreateRequest createTaskRequest;
  WorkflowOuterClass.WorkflowCreateRequest createWorkflowRequest;
  LaunchPlanOuterClass.LaunchPlanCreateRequest createLaunchPlanRequest;
  ExecutionOuterClass.ExecutionCreateRequest createExecutionRequest;

  @Override
  public void createTask(
      TaskOuterClass.TaskCreateRequest request,
      StreamObserver<TaskOuterClass.TaskCreateResponse> responseObserver) {
    this.createTaskRequest = request;
    responseObserver.onNext(TaskOuterClass.TaskCreateResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void createWorkflow(
      WorkflowOuterClass.WorkflowCreateRequest request,
      StreamObserver<WorkflowOuterClass.WorkflowCreateResponse> responseObserver) {
    this.createWorkflowRequest = request;
    responseObserver.onNext(WorkflowOuterClass.WorkflowCreateResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void createLaunchPlan(
      LaunchPlanOuterClass.LaunchPlanCreateRequest request,
      StreamObserver<LaunchPlanOuterClass.LaunchPlanCreateResponse> responseObserver) {
    this.createLaunchPlanRequest = request;
    responseObserver.onNext(LaunchPlanOuterClass.LaunchPlanCreateResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void createExecution(
      ExecutionOuterClass.ExecutionCreateRequest request,
      StreamObserver<ExecutionOuterClass.ExecutionCreateResponse> responseObserver) {
    this.createExecutionRequest = request;
    responseObserver.onNext(ExecutionOuterClass.ExecutionCreateResponse.newBuilder().build());
    responseObserver.onCompleted();
  }
}
