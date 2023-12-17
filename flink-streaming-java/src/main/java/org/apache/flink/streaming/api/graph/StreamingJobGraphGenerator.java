/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return new StreamingJobGraphGenerator(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        null,
                        Runnable::run
                )
                .createJobGraph();
    }



    public static JobGraph createJobGraph(ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
        // TODO Currently, we construct a new thread pool for the compilation of each job. In the
        // future, we may refactor the job submission framework and make it reusable across jobs.
        final ExecutorService serializationExecutor = Executors.newFixedThreadPool(
                        Math.max(1, Math.min(Hardware.getNumberCPUCores(), streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io")
                );
        try {
            return new StreamingJobGraphGenerator(userClassLoader, streamGraph, jobID, serializationExecutor).createJobGraph();
        } finally {
            serializationExecutor.shutdown();
        }
    }



    // ------------------------------------------------------------------------

    private final ClassLoader userClassloader;
    private final StreamGraph streamGraph;

    private final Map<Integer, JobVertex> jobVertices;
    private final JobGraph jobGraph;
    private final Collection<Integer> builtVertices;

    private final List<StreamEdge> physicalEdgesInOrder;

    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    private final Map<Integer, StreamConfig> vertexConfigs;
    private final Map<Integer, String> chainedNames;

    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;

    private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private boolean hasHybridResultPartition = false;

    private final Executor serializationExecutor;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs;

    private StreamingJobGraphGenerator(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            @Nullable JobID jobID,
            Executor serializationExecutor
    ) {
        this.userClassloader = userClassloader;
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

        this.jobVertices = new HashMap<>();
        this.builtVertices = new HashSet<>();
        this.chainedConfigs = new HashMap<>();
        this.vertexConfigs = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedInputOutputFormats = new HashMap<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.opIntermediateOutputs = new HashMap<>();

        jobGraph = new JobGraph(jobID, streamGraph.getJobName());
    }

    /**
     * 创建JobGraph
     *
     * @return
     */
    private JobGraph createJobGraph() {
//        前期检查
        preValidate();
        jobGraph.setJobType(streamGraph.getJobType());
        jobGraph.enableApproximateLocalRecovery(streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        // 为当前版本生成确定性hash，以在后期进行检查【遍历流图并生成hash】
        // 数据结构：node id:hash
        Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

        // Generate legacy version hashes for backwards compatibility
        // 生成旧版本hash，以向后兼容【计算遗留hash值】
        List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }


        /**
         * ChavinKing 1: 合并算子
         */
        setChaining(hashes, legacyHashes);


        /**
         * 物理边指的是那些被创建为jobedge的streamedge
         */
        setPhysicalEdges();


        markContainsSourcesOrSinks();

        /**
         * 设置共享组合本地化策略
         */
        setSlotSharingAndCoLocation();

        setManagedMemoryFraction(
                Collections.unmodifiableMap(jobVertices),
                Collections.unmodifiableMap(vertexConfigs),
                Collections.unmodifiableMap(chainedConfigs),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases()
        );

        // 进行检查点相关配置
        configureCheckpointing();

        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

        final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries =
                JobGraphUtils.prepareUserArtifactEntries(
                        streamGraph.getUserArtifacts().stream().collect(Collectors.toMap(e -> e.f0, e -> e.f1)),
                        jobGraph.getJobID()
                );

        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry : distributedCacheEntries.entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        // set the ExecutionConfig last when it has been finalized
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }

        jobGraph.setChangelogStateBackendEnabled(streamGraph.isChangelogStateBackendEnabled());

        addVertexIndexPrefixInVertexName();

        setVertexDescription();

        // Wait for the serialization of operator coordinators and stream config.
        try {
            FutureUtils.combineAll(
                            vertexConfigs.values().stream()
                                    .map(
                                            config ->
                                                    config.triggerSerializationAndReturnFuture(
                                                            serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();

            waitForSerializationFuturesAndUpdateJobVertices();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }

        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        return jobGraph;
    }

    private void waitForSerializationFuturesAndUpdateJobVertices()
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex : coordinatorSerializationFuturesPerJobVertex.entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    private void addVertexIndexPrefixInVertexName() {
        if (!streamGraph.isVertexNameIncludeIndexPrefix()) {
            return;
        }
        final AtomicInteger vertexIndexId = new AtomicInteger(0);
        jobGraph.getVerticesSortedTopologicallyFromSources()
                .forEach(
                        vertex ->
                                vertex.setName(
                                        String.format(
                                                "[vertex-%d]%s",
                                                vertexIndexId.getAndIncrement(),
                                                vertex.getName())));
    }

    private void setVertexDescription() {
        for (Map.Entry<Integer, JobVertex> headOpAndJobVertex : jobVertices.entrySet()) {
            Integer headOpId = headOpAndJobVertex.getKey();
            JobVertex vertex = headOpAndJobVertex.getValue();
            StringBuilder builder = new StringBuilder();
            switch (streamGraph.getVertexDescriptionMode()) {
                case CASCADING:
                    buildCascadingDescription(builder, headOpId, headOpId);
                    break;
                case TREE:
                    buildTreeDescription(builder, headOpId, headOpId, "", true);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Description mode %s not supported",
                                    streamGraph.getVertexDescriptionMode()));
            }
            vertex.setOperatorPrettyName(builder.toString());
        }
    }

    private void buildCascadingDescription(StringBuilder builder, int headOpId, int currentOpId) {
        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(getDescriptionWithChainedSourcesInfo(node));

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        if (chainedOutput.isEmpty()) {
            return;
        }
        builder.append(" -> ");

        boolean multiOutput = chainedOutput.size() > 1;
        if (multiOutput) {
            builder.append("(");
        }
        while (true) {
            Integer outputId = chainedOutput.pollFirst();
            buildCascadingDescription(builder, headOpId, outputId);
            if (chainedOutput.isEmpty()) {
                break;
            }
            builder.append(" , ");
        }
        if (multiOutput) {
            builder.append(")");
        }
    }

    private LinkedList<Integer> getChainedOutputNodes(int headOpId, StreamNode node) {
        LinkedList<Integer> chainedOutput = new LinkedList<>();
        if (chainedConfigs.containsKey(headOpId)) {
            for (StreamEdge edge : node.getOutEdges()) {
                int targetId = edge.getTargetId();
                if (chainedConfigs.get(headOpId).containsKey(targetId)) {
                    chainedOutput.add(targetId);
                }
            }
        }
        return chainedOutput;
    }

    private void buildTreeDescription(
            StringBuilder builder, int headOpId, int currentOpId, String prefix, boolean isLast) {
        // Replace the '-' in prefix of current node with ' ', keep ':'
        // HeadNode
        // :- Node1
        // :  :- Child1
        // :  +- Child2
        // +- Node2
        //    :- Child3
        //    +- Child4
        String currentNodePrefix = "";
        String childPrefix = "";
        if (currentOpId != headOpId) {
            if (isLast) {
                currentNodePrefix = prefix + "+- ";
                childPrefix = prefix + "   ";
            } else {
                currentNodePrefix = prefix + ":- ";
                childPrefix = prefix + ":  ";
            }
        }

        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(currentNodePrefix);
        builder.append(getDescriptionWithChainedSourcesInfo(node));
        builder.append("\n");

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        while (!chainedOutput.isEmpty()) {
            Integer outputId = chainedOutput.pollFirst();
            buildTreeDescription(builder, headOpId, outputId, childPrefix, chainedOutput.isEmpty());
        }
    }

    private String getDescriptionWithChainedSourcesInfo(StreamNode node) {

        List<StreamNode> chainedSources;
        if (!chainedConfigs.containsKey(node.getId())) {
            // node is not head operator of a vertex
            chainedSources = Collections.emptyList();
        } else {
            chainedSources =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .filter(
                                    id ->
                                            streamGraph.getSourceIDs().contains(id)
                                                    && chainedConfigs
                                                            .get(node.getId())
                                                            .containsKey(id))
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
        }
        return chainedSources.isEmpty()
                ? node.getOperatorDescription()
                : String.format(
                        "%s [%s]",
                        node.getOperatorDescription(),
                        chainedSources.stream()
                                .map(StreamNode::getOperatorDescription)
                                .collect(Collectors.joining(", ")));
    }

    @SuppressWarnings("deprecation")
    private void preValidate() {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
                throw new UnsupportedOperationException(
                        "Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
                                + "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
                                + "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
            }
            if (streamGraph.isIterative()
                    && checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, "
                                + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()
                    && streamGraph.getStreamNodes().stream().anyMatch(this::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, "
                                + "as rescaling is not guaranteed to work correctly."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass =
                            operatorFactory.getStreamOperatorClass(userClassloader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:"
                                        + operatorClass.getName());
                    }
                }
            }
        }

        if (checkpointConfig.isUnalignedCheckpointsEnabled()
                && getCheckpointingMode(checkpointConfig) != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("Unaligned checkpoints can only be used with checkpointing mode EXACTLY_ONCE");
            checkpointConfig.enableUnalignedCheckpoints(false);
        }
    }

    private boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream()
                .anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }

    private void setPhysicalEdges() {
        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

        for (StreamEdge edge : physicalEdgesInOrder) {
            int target = edge.getTargetId();

            List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

            inEdges.add(edge);
        }

        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();

            vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
        }
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(final Map<Integer, byte[]> hashes, final List<Map<Integer, byte[]>> legacyHashes) {

//        定义两个数据结构，用来保存遍历后的信息
        final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        // 处理数据源
        // streamGraph.getSourceIDs获得输入StreamNode的集合
        // 把尽可能多的节点加入chain集合
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            // 首节点
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

//            源节点且出边只有1个
//            策略1：sourcenode是数据源node，同时出边是1
            if (sourceNode.getOperatorFactory() instanceof SourceOperatorFactory && sourceNode.getOutEdges().size() == 1) {
                // as long as only NAry ops support this chaining, we need to skip the other parts
                final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0); // 拿到数据源节点出边，出边边数1
                final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId()); // 拿到数据源节点的下一个节点
//                拿到chain策略枚举值 HEAD_WITH_SOURCES
                final ChainingStrategy targetChainingStrategy = target.getOperatorFactory().getChainingStrategy();

                /**
                 * isChainableInput(sourceOutEdge, streamGraph): 判断StreamEdge能否chain到一起，一共9个条件
                 * 1.16头节点不走这个分支 targetChainingStrategy = AWAYS
                 */
                if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES && isChainableInput(sourceOutEdge, streamGraph)) {
//                    拿到某一个源的ID
                    final OperatorID opId = new OperatorID(hashes.get(sourceNodeId));
                    final StreamConfig.SourceInputConfig inputConfig = new StreamConfig.SourceInputConfig(sourceOutEdge);
                    final StreamConfig operatorConfig = new StreamConfig(new Configuration());

//                    设置节点配置信息
                    setVertexConfig(
                            sourceNodeId, // 源node id
                            operatorConfig, // 空配置信息
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyMap()
                    );

                    operatorConfig.setChainIndex(0); // sources are always first 源的索引永远是第一个
                    operatorConfig.setOperatorID(opId);
                    operatorConfig.setOperatorName(sourceNode.getOperatorName());

                    /**
                     * 把符合要求的节点加入chain的源
                     *      sourceNodeId : 数据源的唯一标识
                     *      new ChainedSourceInfo(operatorConfig, inputConfig): 源算子配置
                     */
                    chainedSources.put(sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                    final SourceOperatorFactory<?> sourceOpFact = (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                    final OperatorCoordinator.Provider coord = sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

                    final OperatorChainInfo chainInfo =
//                            将相邻两个节点创建一个chain对象返回
                            chainEntryPoints.computeIfAbsent(  // computeIfAbsent() 方法对 hashMap 中指定 key 的值进行重新计算，如果不存在这个 key，则添加到 hashMap 中。
                                    sourceOutEdge.getTargetId(),// 下一个NodeID
                                    (k) ->
                                            // 将源节点的输出节点加入chainEntryPoints集合中 ，源的下一个节点内部有对象引用到了上一个源节点
                                            new OperatorChainInfo(
                                                    sourceOutEdge.getTargetId(),
                                                    hashes,
                                                    legacyHashes,
                                                    chainedSources, // 将源对象设置到OperatorChainInfo对象中
                                                    streamGraph));
                    chainInfo.addCoordinatorProvider(coord);
                    continue;
                }
            }

//            将本节点独自创建一个chain对象返回，自己引用自己
            chainEntryPoints.put(sourceNodeId, new OperatorChainInfo(sourceNodeId, hashes, legacyHashes, chainedSources, streamGraph));
        }

        // 存储链上的后一个节点集合
        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        /**
         * buildChainedInputsAndGetHeadInputs(hashes, legacyHashes)
         * 构建链输入集合并且获得头部输入,通过
         * chainEntryPoints 返回源节点结合对应的后一个节点集合
         * hashes, legacyHashes:nodeid : hash
         * ** 先插入后一个节点，然后再插入前一个节点
         *
         * 这个方法只会返回sourceNode的集合 ，不会继续向下遍历StreamGraph【即可以只理解为存在一个节点】
         */
        final Map<Integer, OperatorChainInfo> chainEntryPoints = buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);

//        把value取出，排序，返回
        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        // iterate over a copy of the values, because this map gets concurrently modified
//        遍历每一个数据节点，其实根据本次demo代码来看只有一个头节点，及sourcenode节点
        for (OperatorChainInfo info : initialEntryPoints) {
//            创建chain
            createChain( // 把info内部保存的多个node合并成一个vertex
                    info.getStartNodeId(), // 这个标识的是 node1->node2 中的node2节点ID
                    1, // operators start at position 1 because 0 is for chained source inputs
                    info,
                    chainEntryPoints
            );
        }
    }


    /**
     * 返回边的集合
     *
     * @param currentNodeId
     * @param chainIndex
     * @param chainInfo 初始节点信息
     * @param chainEntryPoints
     * @return
     */
    private List<StreamEdge> createChain(
            final Integer currentNodeId,
            final int chainIndex,
            final OperatorChainInfo chainInfo, // 初始是chainEntryPoints的value
            final Map<Integer, OperatorChainInfo> chainEntryPoints
    ) {

        Integer startNodeId = chainInfo.getStartNodeId(); // 1.得到chain链的开始节点

        if (!builtVertices.contains(startNodeId)) { // 2.如果编译过的节点集合包含这个节点ID,跳过，否则进行操作

//            转换后出边集合
            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
            // 相邻节点可以chain集合
            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            // 相邻节点不可以chain集合
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

//            得到当前节点信息
            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

            /**
             * 遍历处理当前节点的出边信息，将可以chain的存储到一个集合，不能chain的存储到一个集合
             */
            for (StreamEdge outEdge : currentNode.getOutEdges()) { // 遍历节点出边集合
                if (isChainable(outEdge, streamGraph)) { // 如果可以chain把出边加入到 chainableOutputs 集合
                    chainableOutputs.add(outEdge); // 将可以chain的边集合到一起
                } else { // 如果不允许chain，把出边加入到nonChainableOutputs集合
                    nonChainableOutputs.add(outEdge);
                }
            }

// -------------------------------------------------------------------------------------------------
// 构造出边集合，可chain的startnodeid和不可chain的startnodeid都会拿到不可chain两个节点中间的出边作为chain的出边数据
// -------------------------------------------------------------------------------------------------
            // 遍历处理可chain集合,把能chain的节点都放入到 transitiveOutEdges 集合
            // 这个方法会把相邻可以chain的node都组合到一起，遍历后是个图
            for (StreamEdge chainable : chainableOutputs) { // 能chain的
                transitiveOutEdges.addAll( // 这个操作会将从chain起点开始后可以chain的边都收集到当前集合中
                        createChain( // 递归操作，会返回chain的最后一个边作为出边
                                chainable.getTargetId(), // 获取下一个节点
                                chainIndex + 1, // chain链内存在 1 2 3 ... ...
                                chainInfo, // 当能chain的时候，chainInfo要保留为头结点的node信息
                                chainEntryPoints
                        )
                );
            }

            // 遍历处理不可chain集合，遍历后是个图
            for (StreamEdge nonChainable : nonChainableOutputs) { // 不能chain的
                transitiveOutEdges.add(nonChainable); // 这里会将不能chain的边存入集合中
                createChain( // 迭代操作，开启下一个chain链的计算
                        nonChainable.getTargetId(),
                        1, // operators start at position 1 because 0 is for chained source inputs 【操作符从位置1开始，因为0是链式源输入】
                        chainEntryPoints.computeIfAbsent( // 当不能chain时，要更新头结点信息为下一个节点信息，同时会更新chainEntryPoints集合加入start节点信息
                                nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())
                        ),
                        chainEntryPoints
                );
            }
// -------------------------------------------------------------------------------------------------

//            每一个chain链都会被计算
            chainedNames.put(
                    currentNodeId,
                    // 生成名称
                    createChainedName(
                            currentNodeId,
                            chainableOutputs,
                            Optional.ofNullable(chainEntryPoints.get(currentNodeId))
                    )
            );

            chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
            chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));


            /**
             * 将chain内的算子合并起来
             * chainInfo：起始节点
             */
            OperatorID currentOperatorId = chainInfo.addNodeToChain(currentNodeId, streamGraph.getStreamNode(currentNodeId).getOperatorName());


            if (currentNode.getInputFormat() != null) {
                getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }


//            创建JobVertex节点
            StreamConfig config =
                    currentNodeId.equals(startNodeId) // 当前节点等于开始节点，如果相等说明要么就是单独不能chain的边，要么说明chain链已经迭代到最后一层
                            ? createJobVertex(startNodeId, chainInfo) // 在这里将chain的节点信息保存在了operatorIDs内
                            : new StreamConfig(new Configuration()); // 空对象

//            配置节点配置数据，这内部也对中间数据集进行了操作
            setVertexConfig(
                    currentNodeId,
                    config,
                    chainableOutputs,
                    nonChainableOutputs,
                    chainInfo.getChainedSources()
            );


//            说明是chain起点
            if (currentNodeId.equals(startNodeId)) {

                config.setChainStart(); // 设置chain起点
                config.setChainIndex(chainIndex); // 设置chain索引
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName()); // 设置算子名称

//                传递输出
                LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
                for (StreamEdge edge : transitiveOutEdges) { // 将开始chain节点对应的每一个出边都连接起来，在图中每一条链上的出边都会返回最后一个节点的出边作为chain的出边
                    NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
                    transitiveOutputs.add(output);
//                    把两个node连接起来
                    connect(startNodeId, edge, output);
                }

                config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
                config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

            } else { // 说明是chain中间节点，这部分工作对整体流程影响不大

//                起始节点存在则什么都不做，如果不存在则初始化
                chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

                config.setChainIndex(chainIndex);
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                config.setOperatorName(node.getOperatorName());
                chainedConfigs.get(startNodeId).put(currentNodeId, config); // 把当前节点加入开始节点chained集合
            }


            config.setOperatorID(currentOperatorId);

            // 如果出边集合为空，那么chain结束
            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }

            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
        return chainedInputOutputFormats.computeIfAbsent(
                startNodeId,
                k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
    }

    /**
     * 创建chainedname
     *
     * @param vertexID
     * @param chainedOutputs
     * @param operatorChainInfo
     * @return
     */
    private String createChainedName(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            Optional<OperatorChainInfo> operatorChainInfo) {

        final String operatorName =
                nameWithChainedSourcesInfo(
                        streamGraph.getStreamNode(vertexID).getOperatorName(),
                        operatorChainInfo
                                .map(chain -> chain.getChainedSources().values())
                                .orElse(Collections.emptyList())
                );

        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }

    }

    private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
        }
        return minResources;
    }

    private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec preferredResources =
                streamGraph.getStreamNode(vertexID).getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources =
                    preferredResources.merge(
                            chainedPreferredResources.get(chainable.getTargetId()));
        }
        return preferredResources;
    }

    /**
     * 创建JobVertex节点信息
     *
     * @param streamNodeId
     * @param chainInfo
     * @return
     */
    private StreamConfig createJobVertex(Integer streamNodeId, OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId); // 开始节点信息

        byte[] hash = chainInfo.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

//        获取开始节点对应的算子集合，集合内存储的是hash值
        List<Tuple2<byte[], byte[]>> chainedOperators = chainInfo.getChainedOperatorHashes(streamNodeId);

//        这里边也有null值存在
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();

        if (chainedOperators != null) {
            for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID = chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
                operatorIDPairs.add(OperatorIDPair.of(new OperatorID(chainedOperator.f0), userDefinedOperatorID));
            }
        }


        if (chainedInputOutputFormats.containsKey(streamNodeId)) {
//            operatorIDPairs 包含了节点内chained的算子集合
            jobVertex = new InputOutputFormatVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);

            chainedInputOutputFormats
                    .get(streamNodeId)
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex = new JobVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>> serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider : chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    chainedNames.get(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor
                    )
            );
        }

        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVertices.put(streamNodeId, jobVertex);
        builtVertices.add(streamNodeId);
//        将jobverties加入集合
        jobGraph.addVertex(jobVertex);

        return new StreamConfig(jobVertex.getConfiguration());
    }

    /**
     * 设置节点配置信息
     *
     * @param vertexID
     * @param config
     * @param chainableOutputs
     * @param nonChainableOutputs
     * @param chainedSources
     */
    private void setVertexConfig(
            Integer vertexID, // 当前节点，不一定是chain起始节点
            StreamConfig config,
            List<StreamEdge> chainableOutputs,
            List<StreamEdge> nonChainableOutputs,
            Map<Integer, ChainedSourceInfo> chainedSources) {

//        如果是源节点，什么都不做；否则设置每一个边的分区类型
        tryConvertPartitionerForDynamicGraph(chainableOutputs, nonChainableOutputs);

//        拿到传入节点信息，可能是起始节点，也可能不是
        StreamNode vertex = streamGraph.getStreamNode(vertexID);

//        设置节点ID到配置中
        config.setVertexID(vertexID);


        // build the inputs as a combination of source and network inputs
        final List<StreamEdge> inEdges = vertex.getInEdges(); // 拿到传入节点的入边数据
        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();
        final StreamConfig.InputConfig[] inputConfigs = new StreamConfig.InputConfig[inputSerializers.length];

//        设置输入配置信息
        int inputGateCount = 0;
        for (final StreamEdge inEdge : inEdges) {

            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex =
                    inEdge.getTypeNumber() == 0
                            ? 0 // single input operator
                            : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException(
                            "Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                chainedConfigs
                        .computeIfAbsent(vertexID, (key) -> new HashMap<>())
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement =
                            vertex.getInputRequirements()
                                    .getOrDefault(inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] =
                            new StreamConfig.NetworkInputConfig(
                                    inputSerializers[inputIndex],
                                    inputGateCount++,
                                    inputRequirement
                            );
                }
            }
        }

        // set the input config of the vertex if it consumes from cached intermediate dataset.
        if (vertex.getConsumeClusterDatasetId() != null) {
            config.setNumberOfNetworkInputs(1);
            inputConfigs[0] = new StreamConfig.NetworkInputConfig(inputSerializers[0], 0);
        }

        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(streamGraph.getExecutionConfig()));
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(streamGraph.getExecutionConfig()));
            }
        }

        config.setStreamOperatorFactory(vertex.getOperatorFactory());


//        可以重用非链式输出吗
        List<NonChainedOutput> deduplicatedOutputs = mayReuseNonChainedOutputs(vertexID, nonChainableOutputs);


        config.setNumberOfOutputs(deduplicatedOutputs.size());
        config.setOperatorNonChainedOutputs(deduplicatedOutputs);
        config.setChainedOutputs(chainableOutputs); // 设置可chain的边集合



        config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setStateBackend(streamGraph.getStateBackend());
        config.setCheckpointStorage(streamGraph.getCheckpointStorage());
        config.setSavepointDir(streamGraph.getSavepointDirectory());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
        config.getConfiguration()
                .set(
                        ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                        streamGraph.isEnableCheckpointsAfterTasksFinish()
                );
        config.setCheckpointMode(getCheckpointingMode(checkpointCfg));
        config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        config.setAlignedCheckpointTimeout(checkpointCfg.getAlignedCheckpointTimeout());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        config.setStateKeySerializer(vertex.getStateKeySerializer());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class) || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexID));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
        }

        vertexConfigs.put(vertexID, config);
    }

    /**
     * 这个方法会在chain尾节点位置工作
     *
     * @param vertexId 当前节点
     * @param consumerEdges 不可chain边的集合
     * @return
     */
    private List<NonChainedOutput> mayReuseNonChainedOutputs(int vertexId, List<StreamEdge> consumerEdges) {

        /*
            如何不可chain边集合为null，说明vertexId还不是chain尾节点，那么什么都不做
         */
        if (consumerEdges.isEmpty()) {
            return new ArrayList<>();
        }

        List<NonChainedOutput> outputs = new ArrayList<>(consumerEdges.size());
        // 获取尾节点对应的 Map<StreamEdge, NonChainedOutput>，如果存在直接拿出来，如果不存在则创建一个空的
        Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge = opIntermediateOutputs.computeIfAbsent(vertexId, ignored -> new HashMap<>());

        for (StreamEdge consumerEdge : consumerEdges) {
            checkState(vertexId == consumerEdge.getSourceId(), "Vertex id must be the same.");

            int consumerParallelism = streamGraph.getStreamNode(consumerEdge.getTargetId()).getParallelism(); // 获取下一个节点并行度
            int consumerMaxParallelism = streamGraph.getStreamNode(consumerEdge.getTargetId()).getMaxParallelism(); // 获取下一个节点最大并行度

            StreamPartitioner<?> partitioner = consumerEdge.getPartitioner();
            ResultPartitionType partitionType = getResultPartitionType(consumerEdge);

//            中间数据集ID
            IntermediateDataSetID dataSetId = new IntermediateDataSetID();

            boolean isPersistentDataSet = isPersistentIntermediateDataset(partitionType, consumerEdge); // 是否持久化数据集
            if (isPersistentDataSet) {
                partitionType = ResultPartitionType.BLOCKING_PERSISTENT;
                dataSetId = consumerEdge.getIntermediateDatasetIdToProduce();
            }

//            生成NonChainedOutput
            NonChainedOutput output =
                    new NonChainedOutput(
                            consumerEdge.supportsUnalignedCheckpoints(),
                            consumerEdge.getSourceId(), // 当前节点
                            consumerParallelism,
                            consumerMaxParallelism,
                            consumerEdge.getBufferTimeout(),
                            isPersistentDataSet,
                            dataSetId,
                            consumerEdge.getOutputTag(),
                            partitioner,
                            partitionType);

            if (!partitionType.isReconsumable()) {
//                将结果保存起来
                outputs.add(output);
                outputsConsumedByEdge.put(consumerEdge, output); // 更新 opIntermediateOutputs 数据结构，这里用的事不可chain的边作为key进行存储的
            } else {
                NonChainedOutput reusableOutput = null;
                for (NonChainedOutput outputCandidate : outputsConsumedByEdge.values()) {
                    // the target output can be reused if they have the same partitioner and
                    // consumer parallelism, reusing the same output can improve performance
                    if (
                            outputCandidate.getPartitionType().isReconsumable()
                            && consumerParallelism == outputCandidate.getConsumerParallelism()
                            && consumerMaxParallelism == outputCandidate.getConsumerMaxParallelism()
                            && outputCandidate.getPartitionType() == partitionType
                            && Objects.equals(
                                    outputCandidate.getPersistentDataSetId(),
                                    consumerEdge.getIntermediateDatasetIdToProduce())
                            && Objects.equals(outputCandidate.getOutputTag(), consumerEdge.getOutputTag())
                            && Objects.equals(partitioner, outputCandidate.getPartitioner())
                    ) {
                        reusableOutput = outputCandidate;
                        outputsConsumedByEdge.put(consumerEdge, reusableOutput); // 更新 opIntermediateOutputs 数据结构
                        break;
                    }
                }
                if (reusableOutput == null) {
                    outputs.add(output);
                    outputsConsumedByEdge.put(consumerEdge, output); // 更新 opIntermediateOutputs 数据结构
                }
            }
        }
        return outputs;
    }

    private void tryConvertPartitionerForDynamicGraph(List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

        for (StreamEdge edge : chainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner || partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.getExecutionConfig().isDynamicGraph(),
                        String.format(
                                "%s should only be used in dynamic graph.",
                                partitioner.getClass().getSimpleName()));
                edge.setPartitioner(new ForwardPartitioner<>());
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                checkState(
                        streamGraph.getExecutionConfig().isDynamicGraph(),
                        "ForwardForConsecutiveHashPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(((ForwardForConsecutiveHashPartitioner<?>) partitioner).getHashPartitioner());
            } else if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.getExecutionConfig().isDynamicGraph(),
                        "ForwardForUnspecifiedPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(new RescalePartitioner<>());
            }
        }
    }

    private CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
        CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingMode();

        checkArgument(
                checkpointingMode == CheckpointingMode.EXACTLY_ONCE
                        || checkpointingMode == CheckpointingMode.AT_LEAST_ONCE,
                "Unexpected checkpointing mode.");

        if (checkpointConfig.isCheckpointingEnabled()) {
            return checkpointingMode;
        } else {
            // the "at-least-once" input handler is slightly cheaper (in the absence of
            // checkpoints),
            // so we use that one if checkpointing is not enabled
            return CheckpointingMode.AT_LEAST_ONCE;
        }
    }

    /**
     * 将 chain头 + 边 + 中间结果集 连接起来；chain链头才会触发；
     * 构造 JobVertex + IntermediateDataSetID + jobEdge + JobVertex 链
     *
     * 如果 edge 是不可chain的，那么output有实例对象存在，否则output为空
     *
     *
     * @param headOfChain
     * @param edge ：chain链的最后一个节点的出边
     * @param output
     */
    private void connect(Integer headOfChain, StreamEdge edge, NonChainedOutput output) {

        physicalEdgesInOrder.add(edge);

        Integer downStreamVertexID = edge.getTargetId();// 下一个节点id
        JobVertex headVertex = jobVertices.get(headOfChain); // 头节点
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID); // 下一个节点

        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);

        StreamPartitioner<?> partitioner = output.getPartitioner();
        ResultPartitionType resultPartitionType = output.getPartitionType();

        if (resultPartitionType == ResultPartitionType.HYBRID_FULL || resultPartitionType == ResultPartitionType.HYBRID_SELECTIVE) {
            hasHybridResultPartition = true;
        }

        checkBufferTimeout(resultPartitionType, edge);

        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType, // 分区类型 output
                            opIntermediateOutputs.get(edge.getSourceId()).get(edge).getDataSetId(), // headVertex节点数据集ID
                            partitioner.isBroadcast() // output
                    );
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            opIntermediateOutputs.get(edge.getSourceId()).get(edge).getDataSetId(),
                            partitioner.isBroadcast()
                    );
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
        jobEdge.setForward(partitioner instanceof ForwardPartitioner);
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CONNECTED: {} - {} -> {}",
                    partitioner.getClass().getSimpleName(),
                    headOfChain,
                    downStreamVertexID);
        }
    }

    private boolean isPersistentIntermediateDataset(ResultPartitionType resultPartitionType, StreamEdge edge) {
        return resultPartitionType.isBlockingOrBlockingPersistentResultPartition() && edge.getIntermediateDatasetIdToProduce() != null;
    }

    private void checkBufferTimeout(ResultPartitionType type, StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (!type.canBePipelinedConsumed() && bufferTimeout != ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "only canBePipelinedConsumed partition support buffer timeout "
                            + bufferTimeout
                            + " for src operator in edge "
                            + edge
                            + ". \nPlease either disable buffer timeout (via -1) or use the canBePipelinedConsumed partition.");
        }
    }

    private ResultPartitionType getResultPartitionType(StreamEdge edge) {
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case BATCH:
                return ResultPartitionType.BLOCKING;
            case HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            case UNDEFINED:
                return determineUndefinedResultPartitionType(edge.getPartitioner());
            default:
                throw new UnsupportedOperationException(
                        "Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }
    }

    private ResultPartitionType determineUndefinedResultPartitionType(
            StreamPartitioner<?> partitioner) {
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            case ALL_EDGES_HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case ALL_EDGES_HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            default:
                throw new RuntimeException(
                        "Unrecognized global data exchange mode "
                                + streamGraph.getGlobalStreamExchangeMode());
        }
    }

    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);// 获取出边对应的目标节点
        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph); // isChainableInput(edge, streamGraph) 用来判断出边是否可以合并
    }

    /**
     * 这个方法用来判断相邻的两个StreamNode能否被chain到一起
     *
     * @param edge
     * @param streamGraph
     * @return
     */
    private static boolean isChainableInput(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge); // 拿到出边对应的源节点信息
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge); // 拿到出边对应的目标节点信息

        if (!(upStreamVertex.isSameSlotSharingGroup(downStreamVertex) // 条件1 上游StreamNode和下游StreamNode要在一个资源共享组里
                && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
                && arePartitionerAndExchangeModeChainable(
                        edge.getPartitioner(),
                        edge.getExchangeMode(),
                        streamGraph.getExecutionConfig().isDynamicGraph())
                && upStreamVertex.getParallelism() == downStreamVertex.getParallelism() // 条件1 上游并行度要等于下游并行度
                && streamGraph.isChainingEnabled() // 条件3：用户启动了chain服务
        )) {

            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) { // 条件9：拿到下游入边
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        // 条件8：窄依赖可以chain到一起
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(StreamNode upStreamVertex, StreamNode downStreamVertex, StreamGraph streamGraph) {
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();

        // 条件4： 上游算子和下游算子都不能为空
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this
        // point
        // 条件5： source直接对应sink不能chain
        if (downStreamOperator instanceof YieldingOperatorFactory && getHeadOperator(upStreamVertex, streamGraph).isLegacySource()) {
            return false;
        }

        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        // 条件6：上游chain策略不能是NEVER
        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        // 条件7：下游chain策略不能是NEVER，HEAD
        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + downStreamOperator.getChainingStrategy());
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(
            StreamNode upStreamVertex, StreamGraph streamGraph) {
        if (upStreamVertex.getInEdges().size() == 1
                && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
            return getHeadOperator(
                    streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)), streamGraph);
        }
        return upStreamVertex.getOperatorFactory();
    }

    private void markContainsSourcesOrSinks() {

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex jobVertex = entry.getValue();
            final Set<Integer> vertexOperators = new HashSet<>();

            vertexOperators.add(entry.getKey());

            if (chainedConfigs.containsKey(entry.getKey())) {
                vertexOperators.addAll(chainedConfigs.get(entry.getKey()).keySet());
            }

            for (int nodeId : vertexOperators) {
                if (streamGraph.getSourceIDs().contains(nodeId)) {
                    jobVertex.markContainsSources();
                }
                if (streamGraph.getSinkIDs().contains(nodeId) || streamGraph.getExpandedSinkIds().contains(nodeId)) {
                    jobVertex.markContainsSinks();
                }
            }
        }
    }

    private void setSlotSharingAndCoLocation() {
        setSlotSharing();
        setCoLocation();
    }

    private void setSlotSharing() {
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups =
                buildVertexRegionSlotSharingGroups();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey =
                    streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup =
                        checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                checkState(
                        !hasHybridResultPartition,
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

                effectiveSlotSharingGroup =
                        specifiedSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupKey,
                                k -> {
                                    SlotSharingGroup ssg = new SlotSharingGroup();
                                    streamGraph
                                            .getSlotSharingGroupResource(k)
                                            .ifPresent(ssg::setResourceProfile);
                                    return ssg;
                                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup =
                streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        final Iterable<DefaultLogicalPipelinedRegion> regions =
                DefaultLogicalTopology.fromJobGraph(jobGraph).getAllPipelinedRegions();
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private void setCoLocation() {
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups =
                new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException(
                            "Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint =
                        coLocationGroups.computeIfAbsent(
                                coLocationGroupKey,
                                k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException(
                            "Cannot co-locate operators from different slot sharing groups");
                }

                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    private static void setManagedMemoryFraction(
            final Map<Integer, JobVertex> jobVertices,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(
                    jobVertexSlotSharingGroup != null,
                    "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(
                    vertexChainedConfigs
                            .getOrDefault(headOperatorId, Collections.emptyMap())
                            .keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(
                    slotSharingGroup,
                    vertexHeadOperators,
                    vertexOperators,
                    operatorConfigs,
                    vertexChainedConfigs,
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
                    slotScopeManagedMemoryUseCasesRetriever);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(
            final SlotSharingGroup slotSharingGroup,
            final Map<JobVertexID, Integer> vertexHeadOperators,
            final Map<JobVertexID, Set<Integer>> vertexOperators,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        final Set<Integer> groupOperatorIds =
                slotSharingGroup.getJobVertexIds().stream()
                        .flatMap((vid) -> vertexOperators.get(vid).stream())
                        .collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(oid)
                                                .entrySet().stream())
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getKey,
                                        Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        slotScopeManagedMemoryUseCasesRetriever.apply(oid).stream())
                        .collect(Collectors.toSet());

        for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights =
                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(operatorNodeId);
                final Set<ManagedMemoryUseCase> slotScopeUseCases =
                        slotScopeManagedMemoryUseCasesRetriever.apply(operatorNodeId);
                setManagedMemoryFractionForOperator(
                        operatorScopeUseCaseWeights,
                        slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights,
                        groupSlotScopeUseCases,
                        operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
            vertexConfig.setTransitiveChainedTaskConfigs(
                    vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(
            final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            final Set<ManagedMemoryUseCase> slotScopeUseCases,
            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase,
                    operatorWeight > 0
                            ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight)
                            : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }




    private void configureCheckpointing() {
        CheckpointConfig cfg = streamGraph.getCheckpointConfig();

        long interval = cfg.getCheckpointInterval();
        if (interval < MINIMAL_CHECKPOINT_TIME) {
            // interval of max value means disable periodic checkpoint
            interval = Long.MAX_VALUE;
        }

        //  --- configure options ---
        CheckpointRetentionPolicy retentionAfterTermination;
        if (cfg.isExternalizedCheckpointsEnabled()) {
            CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
            // Sanity check
            if (cleanup == null) {
                throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
            }
            retentionAfterTermination = cleanup.deleteOnCancellation()
                            ? CheckpointRetentionPolicy.RETAIN_ON_FAILURE
                            : CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
        } else {
            retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        }

        //  --- configure the master-side checkpoint hooks ---
        final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

        for (StreamNode node : streamGraph.getStreamNodes()) {
            if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();
                if (f instanceof WithMasterCheckpointHook) {
                    hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
                }
            }
        }

        // because the hooks can have user-defined code, they need to be stored as
        // eagerly serialized values
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
        if (hooks.isEmpty()) {
            serializedHooks = null;
        } else {
            try {
                MasterTriggerRestoreHook.Factory[] asArray = hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
                serializedHooks = new SerializedValue<>(asArray);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
            }
        }

        // because the state backend can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<StateBackend> serializedStateBackend;
        if (streamGraph.getStateBackend() == null) {
            serializedStateBackend = null;
        } else {
            try {
                serializedStateBackend = new SerializedValue<StateBackend>(streamGraph.getStateBackend());
            } catch (IOException e) {
                throw new FlinkRuntimeException("State backend is not serializable", e);
            }
        }

        // because the checkpoint storage can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<CheckpointStorage> serializedCheckpointStorage;
        if (streamGraph.getCheckpointStorage() == null) {
            serializedCheckpointStorage = null;
        } else {
            try {
                serializedCheckpointStorage = new SerializedValue<>(streamGraph.getCheckpointStorage());
            } catch (IOException e) {
                throw new FlinkRuntimeException("Checkpoint storage is not serializable", e);
            }
        }

        //  --- done, put it all together ---
        JobCheckpointingSettings settings =
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(interval)
                                .setCheckpointTimeout(cfg.getCheckpointTimeout())
                                .setMinPauseBetweenCheckpoints(cfg.getMinPauseBetweenCheckpoints())
                                .setMaxConcurrentCheckpoints(cfg.getMaxConcurrentCheckpoints())
                                .setCheckpointRetentionPolicy(retentionAfterTermination)
                                .setExactlyOnce(getCheckpointingMode(cfg) == CheckpointingMode.EXACTLY_ONCE)
                                .setTolerableCheckpointFailureNumber(cfg.getTolerableCheckpointFailureNumber())
                                .setUnalignedCheckpointsEnabled(cfg.isUnalignedCheckpointsEnabled())
                                .setCheckpointIdOfIgnoredInFlightData(cfg.getCheckpointIdOfIgnoredInFlightData())
                                .setAlignedCheckpointTimeout(cfg.getAlignedCheckpointTimeout().toMillis())
                                .setEnableCheckpointsAfterTasksFinish(streamGraph.isEnableCheckpointsAfterTasksFinish())
                                .build(),
                        serializedStateBackend,
                        streamGraph.isChangelogStateBackendEnabled(),
                        serializedCheckpointStorage,
                        serializedHooks
                );

        jobGraph.setSnapshotSettings(settings);
    }




    private static String nameWithChainedSourcesInfo(String operatorName, Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty()
                ? operatorName
                : String.format(
                        "%s [%s]",
                        operatorName,
                        chainedSourceInfos.stream()
                                .map(
                                        chainedSourceInfo ->
                                                chainedSourceInfo
                                                        .getOperatorConfig()
                                                        .getOperatorName())
                                .collect(Collectors.joining(", ")));
    }

    /**
     * A private class to help maintain the information of an operator chain during the recursive
     * call in {@link #createChain(Integer, int, OperatorChainInfo, Map)}.
     */
    private static class OperatorChainInfo {
        private final Integer startNodeId;
        private final Map<Integer, byte[]> hashes;
        private final List<Map<Integer, byte[]>> legacyHashes;

//        用来保存每一个头节点内部覆盖的算子信息
        private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
        private final Map<Integer, ChainedSourceInfo> chainedSources;
        private final List<OperatorCoordinator.Provider> coordinatorProviders;
        private final StreamGraph streamGraph;

        private OperatorChainInfo(
                int startNodeId,
                Map<Integer, byte[]> hashes,
                List<Map<Integer, byte[]>> legacyHashes,
                Map<Integer, ChainedSourceInfo> chainedSources,
                StreamGraph streamGraph) {
            this.startNodeId = startNodeId;
            this.hashes = hashes;
            this.legacyHashes = legacyHashes;
            this.chainedOperatorHashes = new HashMap<>();
            this.coordinatorProviders = new ArrayList<>();
            this.chainedSources = chainedSources;
            this.streamGraph = streamGraph;
        }

        byte[] getHash(Integer streamNodeId) {
            return hashes.get(streamNodeId);
        }

        private Integer getStartNodeId() {
            return startNodeId;
        }

        private List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
            return chainedOperatorHashes.get(startNodeId);
        }

        void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
            coordinatorProviders.add(coordinator);
        }

        private List<OperatorCoordinator.Provider> getCoordinatorProviders() {
            return coordinatorProviders;
        }

        Map<Integer, ChainedSourceInfo> getChainedSources() {
            return chainedSources;
        }

        /**
         * 添加节点到chain
         *
         * @param currentNodeId
         * @param operatorName
         * @return
         */
        private OperatorID addNodeToChain(int currentNodeId, String operatorName) {
//            Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
            /**
             * computeIfAbsent()
             * 1、首先会判断map中是否有对应的Key；
             * 2.1、如果没有对应的Key，则会创建一个满足Value类型的数据结构放到Value的位置中；
             * 2.2、如果有对应的Key，则会操作该Key对应的Value.
             */
            List<Tuple2<byte[], byte[]>> operatorHashes = chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

//            获取当前节点hash值
            byte[] primaryHashBytes = hashes.get(currentNodeId);

//            这里不是把legacyHashes中的所有的变量【value为null】都要add到operatorHashes
//            legacyHashes指的是旧的hash集合，只有一个null值
            for (Map<Integer, byte[]> legacyHash : legacyHashes) {
                operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
            }

            streamGraph
                    .getStreamNode(currentNodeId)
                    .getCoordinatorProvider(operatorName, new OperatorID(getHash(currentNodeId)))
                    .map(coordinatorProviders::add);

            return new OperatorID(primaryHashBytes);
        }


        private OperatorChainInfo newChain(Integer startNodeId) {
            return new OperatorChainInfo(startNodeId, hashes, legacyHashes, chainedSources, streamGraph);
        }
    }

    private static final class ChainedSourceInfo {
        private final StreamConfig operatorConfig;
        private final StreamConfig.SourceInputConfig inputConfig;

        ChainedSourceInfo(StreamConfig operatorConfig, StreamConfig.SourceInputConfig inputConfig) {
            this.operatorConfig = operatorConfig;
            this.inputConfig = inputConfig;
        }

        public StreamConfig getOperatorConfig() {
            return operatorConfig;
        }

        public StreamConfig.SourceInputConfig getInputConfig() {
            return inputConfig;
        }
    }
}
