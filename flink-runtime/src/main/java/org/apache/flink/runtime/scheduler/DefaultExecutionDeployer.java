/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link ExecutionDeployer}. */
public class DefaultExecutionDeployer implements ExecutionDeployer {

    private final Logger log;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionOperations executionOperations;

    private final ExecutionVertexVersioner executionVertexVersioner;

    private final Time partitionRegistrationTimeout;

    private final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private DefaultExecutionDeployer(
            final Logger log,
            final ExecutionSlotAllocator executionSlotAllocator,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final Time partitionRegistrationTimeout,
            final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
            final ComponentMainThreadExecutor mainThreadExecutor) {

        this.log = checkNotNull(log);
        this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
        this.executionOperations = checkNotNull(executionOperations);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.partitionRegistrationTimeout = checkNotNull(partitionRegistrationTimeout);
        this.allocationReservationFunc = checkNotNull(allocationReservationFunc);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
    }





    /**
     * 启动任务入口方法 DefaultExecutionDeployer.allocateSlotsAndDeploy()
     *
     * @param executionsToDeploy executions to deploy
     * @param requiredVersionByVertex required versions of the execution vertices. If the actual
     *     version does not match, the deployment of the execution will be rejected.
     */
    @Override
    public void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy, // 5个执行器
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex // 版本信息
    ) {

        validateExecutionStates(executionsToDeploy);

        transitionToScheduled(executionsToDeploy);

        // 1-申请资源
        /**
         * 这段代码的作用是根据调度策略为需要部署的任务分配执行 slot，生成 ExecutionSlotAssignment 列表。
         *
         * 在 Flink 中，执行 slot 是 Flink 集群中资源分配的基本单位，用于承载任务的执行。调度器根据任务的资源需求和调度策略，将任务分配给不同的 slot 进行执行。
         *
         * 具体来说，这个方法会将需要部署的任务集合 executionsToDeploy 传入 allocateSlotsFor 方法中，该方法会按照预定义的调度策略为每个任务分配执行 slot。执行 slot 的分配策略一般包括以下几个方面的考虑：
         *
         * 任务资源需求，如 CPU、内存、IO 等
         * 集群资源情况，如空闲的 slot 数量、其他任务占用的 slot 等
         * 任务优先级和调度策略，如 FIFO、FAIR、PRIORITY 等
         * 分配执行 slot 后，该方法会返回一个 List，其中包含了为每个任务分配的执行 slot 的信息，包括任务 ID、分配的 slot ID 等。这些信息将被传递给后续的部署流程，用于将任务分配到相应的 slot 上进行执行。
         */
        final List<ExecutionSlotAssignment> executionSlotAssignments = allocateSlotsFor(executionsToDeploy);


        /**
         * 获取部署句柄地址，这里返回结果封装了执行器，资源类和版本3个参数的类的集合(共5个元素)
         */
        final List<ExecutionDeploymentHandle> deploymentHandles = createDeploymentHandles(executionsToDeploy, requiredVersionByVertex, executionSlotAssignments);

        // 2-调度执行
        waitForAllSlotsAndDeploy(deploymentHandles);
    }








    private void validateExecutionStates(final Collection<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(
                e ->
                        checkState(
                                e.getState() == ExecutionState.CREATED,
                                "Expected execution %s to be in CREATED state, was: %s",
                                e.getAttemptId(),
                                e.getState()));
    }

    private void transitionToScheduled(final List<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(e -> e.transitionState(ExecutionState.SCHEDULED));
    }

    private List<ExecutionSlotAssignment> allocateSlotsFor(final List<Execution> executionsToDeploy) {
        final List<ExecutionAttemptID> executionAttemptIds =
                executionsToDeploy.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
        return executionSlotAllocator.allocateSlotsFor(executionAttemptIds);
    }

    private List<ExecutionDeploymentHandle> createDeploymentHandles(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final List<ExecutionSlotAssignment> executionSlotAssignments
    ) {

        final List<ExecutionDeploymentHandle> deploymentHandles = new ArrayList<>(executionsToDeploy.size());

        for (int i = 0; i < executionsToDeploy.size(); i++) {
            final Execution execution = executionsToDeploy.get(i);
            final ExecutionSlotAssignment assignment = executionSlotAssignments.get(i);
            checkState(execution.getAttemptId().equals(assignment.getExecutionAttemptId()));

            final ExecutionVertexID executionVertexId = execution.getVertex().getID();
            final ExecutionDeploymentHandle deploymentHandle = new ExecutionDeploymentHandle(execution, assignment, requiredVersionByVertex.get(executionVertexId));
            deploymentHandles.add(deploymentHandle);
        }

        return deploymentHandles;
    }



    private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
        FutureUtils.assertNoException(
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles).handle(deployAll(deploymentHandles))
        );
    }



    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(final List<ExecutionDeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        for (ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> resultFuture =
                    deploymentHandle
                            .getLogicalSlotFuture()
                            .handle(assignResource(deploymentHandle)) // 分配资源
                            .thenCompose(registerProducedPartitions(deploymentHandle))
                            .handle(
                                    (ignore, throwable) -> {
                                        if (throwable != null) {
                                            handleTaskDeploymentFailure(
                                                    deploymentHandle.getExecution(), throwable);
                                        }
                                        return null;
                                    });

            resultFutures.add(resultFuture);
        }
        return FutureUtils.waitForAll(resultFutures);
    }


    /**
     *
     * @param deploymentHandles : 封装了 执行器 资源 和 版本信息的集合 共5个元素
     * @return
     */
    private BiFunction<Void, Throwable, Void> deployAll(final List<ExecutionDeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);


            // 遍历一个一个执行
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                final CompletableFuture<LogicalSlot> slotAssigned = deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

//                分别部署每一个任务，一共要执行5个任务
                FutureUtils.assertNoException(slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }


            return null;
        };
    }



    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(final ExecutionDeploymentHandle deploymentHandle) {

        return (logicalSlot, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                    releaseSlotIfPresent(logicalSlot);
                }
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel.
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }

            if (!execution.tryAssignResource(logicalSlot)) {
                throw new IllegalStateException(
                        "Could not assign resource "
                                + logicalSlot
                                + " to execution "
                                + execution
                                + '.');
            }

            // We only reserve the latest execution of an execution vertex. Because it may cause
            // problems to reserve multiple slots for one execution vertex. Besides that, slot
            // reservation is for local recovery and therefore is only needed by streaming jobs, in
            // which case an execution vertex will have one only current execution.
            allocationReservationFunc.accept(
                    execution.getAttemptId().getExecutionVertexId(), logicalSlot.getAllocationId());

            return logicalSlot;
        };
    }

    private static void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(final ExecutionDeploymentHandle deploymentHandle) {

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            if (logicalSlot != null) {
                final Execution execution = deploymentHandle.getExecution();
                final CompletableFuture<Void> partitionRegistrationFuture = execution.registerProducedPartitions(logicalSlot.getTaskManagerLocation());

                return FutureUtils.orTimeout(
                        partitionRegistrationFuture,
                        partitionRegistrationTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS,
                        mainThreadExecutor);
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }

    /**
     * 1/5 分次执行
     *
     * @param deploymentHandle
     * @return
     */
    private BiFunction<Object, Throwable, Void> deployOrHandleError(final ExecutionDeploymentHandle deploymentHandle) {

        return (ignored, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                }
                return null;
            }

            if (throwable == null) {
//                开始部署任务
                deployTaskSafe(execution);
            } else {
                handleTaskDeploymentFailure(execution, throwable);
            }
            return null;
        };
    }




    private void deployTaskSafe(final Execution execution) {
        try {
            executionOperations.deploy(execution);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(execution, e);
        }
    }

    private void handleTaskDeploymentFailure(final Execution execution, final Throwable error) {
        executionOperations.markFailed(execution, error);
    }

    private static class ExecutionDeploymentHandle {

        private final Execution execution;

        private final ExecutionSlotAssignment executionSlotAssignment;

        private final ExecutionVertexVersion requiredVertexVersion;

        ExecutionDeploymentHandle(
                final Execution execution,
                final ExecutionSlotAssignment executionSlotAssignment,
                final ExecutionVertexVersion requiredVertexVersion) {
            this.execution = checkNotNull(execution);
            this.executionSlotAssignment = checkNotNull(executionSlotAssignment);
            this.requiredVertexVersion = checkNotNull(requiredVertexVersion);
        }

        Execution getExecution() {
            return execution;
        }

        ExecutionAttemptID getExecutionAttemptId() {
            return execution.getAttemptId();
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return executionSlotAssignment.getLogicalSlotFuture();
        }

        ExecutionVertexVersion getRequiredVertexVersion() {
            return requiredVertexVersion;
        }
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployer}. */
    public static class Factory implements ExecutionDeployer.Factory {

        @Override
        public DefaultExecutionDeployer createInstance(
                Logger log,
                ExecutionSlotAllocator executionSlotAllocator,
                ExecutionOperations executionOperations,
                ExecutionVertexVersioner executionVertexVersioner,
                Time partitionRegistrationTimeout,
                BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                ComponentMainThreadExecutor mainThreadExecutor) {
            return new DefaultExecutionDeployer(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    partitionRegistrationTimeout,
                    allocationReservationFunc,
                    mainThreadExecutor);
        }
    }
}
