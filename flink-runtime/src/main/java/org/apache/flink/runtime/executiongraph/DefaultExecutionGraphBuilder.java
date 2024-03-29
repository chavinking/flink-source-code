/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionGroupReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link DefaultExecutionGraph} from a {@link
 * JobGraph}.
 */
public class DefaultExecutionGraphBuilder {

    /**
     * 编译executiongraph
     *
     * @param jobGraph
     * @param jobManagerConfig
     * @param futureExecutor
     * @param ioExecutor
     * @param classLoader
     * @param completedCheckpointStore
     * @param checkpointsCleaner
     * @param checkpointIdCounter
     * @param rpcTimeout
     * @param blobWriter
     * @param log
     * @param shuffleMaster
     * @param partitionTracker
     * @param partitionLocationConstraint
     * @param executionDeploymentListener
     * @param executionStateUpdateListener
     * @param initializationTimestamp
     * @param vertexAttemptNumberStore
     * @param vertexParallelismStore
     * @param checkpointStatsTrackerFactory
     * @param isDynamicGraph
     * @param executionJobVertexFactory
     * @return
     * @throws JobExecutionException
     * @throws JobException
     */
    public static DefaultExecutionGraph buildGraph(
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            ClassLoader classLoader,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            Time rpcTimeout,
            BlobWriter blobWriter,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            Supplier<CheckpointStatsTracker> checkpointStatsTrackerFactory,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory)  throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");

        final String jobName = jobGraph.getName();
        final JobID jobId = jobGraph.getJobID();

        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths()
                );

        final int executionHistorySizeLimit = jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory = PartitionGroupReleaseStrategyFactoryLoader.loadPartitionGroupReleaseStrategyFactory(jobManagerConfig);


        // create a new execution graph, if none exists so far
        final DefaultExecutionGraph executionGraph; // 创建executiongraph
        try {
            executionGraph = new DefaultExecutionGraph(
                            jobInformation,
                            futureExecutor,
                            ioExecutor,
                            rpcTimeout,
                            executionHistorySizeLimit,
                            classLoader,
                            blobWriter,
                            partitionGroupReleaseStrategyFactory,
                            shuffleMaster,
                            partitionTracker,
                            partitionLocationConstraint,
                            executionDeploymentListener,
                            executionStateUpdateListener,
                            initializationTimestamp,
                            vertexAttemptNumberStore,
                            vertexParallelismStore,
                            isDynamicGraph,
                            executionJobVertexFactory,
                            jobGraph.getJobStatusHooks()
                    );
        } catch (IOException e) {
            throw new JobException("Could not create the ExecutionGraph.", e);
        }

        // set the basic properties
        try {
            // ********* 将jobgrap转换成json格式
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);


        /**
         * 检查 设置一些配置信息
         */
        for (JobVertex vertex : jobGraph.getVertices()) {
            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {
                vertex.initializeOnMaster(
                        new SimpleInitializeOnMasterContext(
                                classLoader,
                                vertexParallelismStore
                                        .getParallelismInfo(vertex.getID())
                                        .getParallelism()
                        )
                );
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info("Successfully ran initialization on master in {} ms.", (System.nanoTime() - initMasterStart) / 1_000_000);


        // topologically sort the job vertices and attach the graph to the existing one
        // 对作业顶点进行拓扑排序，并将图附加到现有的图上【拍过序的结果】
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId
            );
        }


//        执行准换 ***
//        sortedTopology：排过序的jobvertex列表，即 source -> map -> sink 顺序
        executionGraph.attachJobGraph(sortedTopology);



        if (log.isDebugEnabled()) {
            log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }



        /**
         * JOB任务状态检查点源码
         */
        // configure the state checkpointing
        if (isDynamicGraph) {
            // dynamic graph does not support checkpointing so we skip it
            log.warn("Skip setting up checkpointing for a job with dynamic graph.");
        } else if (isCheckpointingEnabled(jobGraph)) { // 启用检查点，进行检查点检查

            /**
             * 状态后端管理代码，分析执行图可以忽略
             */
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

            /**
             * 1. 从配置信息加载状态后端
             */
            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try { // 初始化状态后端
                    applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend,
                                snapshotSettings.isChangelogStateBackendEnabled(),
                                jobManagerConfig,
                                classLoader,
                                log
                        );
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
            }

            /**
             * 2. 从应用配置加载检查点策略
             */
            // load the checkpoint storage from the application settings
            final CheckpointStorage applicationConfiguredStorage;
            final SerializedValue<CheckpointStorage> serializedAppConfiguredStorage = snapshotSettings.getDefaultCheckpointStorage();

            if (serializedAppConfiguredStorage == null) {
                applicationConfiguredStorage = null;
            } else {
                try {
                    applicationConfiguredStorage = serializedAppConfiguredStorage.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(jobId, "Could not deserialize application-defined checkpoint storage.", e);
                }
            }

            final CheckpointStorage rootStorage;
            try {
                rootStorage = CheckpointStorageLoader.load(
                                applicationConfiguredStorage,
                                null,
                                rootBackend,
                                jobManagerConfig,
                                classLoader,
                                log
                );
            } catch (IllegalConfigurationException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(jobId, "Could not instantiate configured checkpoint storage", e);
            }


            /**
             * 3. 列举用户定义的检查点hooks
             */
            // instantiate the user-defined checkpoint hooks
            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();
            String changelogStorage = jobManagerConfig.getString(STATE_CHANGE_LOG_STORAGE);


            /**
             * 4. 启动检查点协调器
             *
             *
             */
            executionGraph.enableCheckpointing(
                    chkConfig, // 这是用于配置检查点的配置对象，其中包含了与检查点相关的各种配置选项，例如检查点的间隔时间、最大并发检查点数量等。
                    hooks, // 这是一组用于定制化检查点行为的钩子（hook）对象。通过这些钩子，你可以在不同的检查点生命周期事件中插入自定义逻辑，例如在检查点完成之后执行一些特定操作。
                    checkpointIdCounter, // 这是一个计数器，用于生成唯一的检查点 ID。每次执行检查点时，都会生成一个新的检查点 ID。
                    completedCheckpointStore, // 这是一个存储已完成检查点的存储后端。在检查点成功完成后，相关的状态信息会被存储在这个存储后端中，以便在需要时进行恢复。
                    // rootBackend 和 rootStorage: 这两个参数提供了检查点状态的根后端和根存储。这些用于指定检查点数据在分布式文件系统中的存储位置。
                    rootBackend,
                    rootStorage,
                    checkpointStatsTrackerFactory.get(), // 这是一个用于创建检查点统计跟踪器的工厂方法。这个统计跟踪器可以帮助你监视和记录检查点操作的性能指标和统计信息。
                    checkpointsCleaner, // 这是一个用于清理过期检查点的对象。在保留一定数量的检查点之后，你可能需要删除旧的检查点，以释放存储空间。
                    jobManagerConfig.getString(STATE_CHANGE_LOG_STORAGE) // 这个参数指定了状态变更日志的存储位置。状态变更日志用于记录应用程序状态的变更，以便在故障发生时进行精确的状态恢复。
            );

        }

        return executionGraph;
    }



    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private DefaultExecutionGraphBuilder() {}
}
