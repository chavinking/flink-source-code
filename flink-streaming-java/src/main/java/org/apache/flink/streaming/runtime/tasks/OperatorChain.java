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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorChain} contains all operators that are executed as one chain within a single
 * {@link StreamTask}.
 *
 * <p>The main entry point to the chain is it's {@code mainOperator}. {@code mainOperator} is
 * driving the execution of the {@link StreamTask}, by pulling the records from network inputs
 * and/or source inputs and pushing produced records to the remaining chained operators.
 *
 * @param <OUT> The type of elements accepted by the chain, i.e., the input type of the chain's main
 *     operator.
 */
public abstract class OperatorChain<OUT, OP extends StreamOperator<OUT>>
        implements BoundedMultiInput, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);

    protected final RecordWriterOutput<?>[] streamOutputs;

    protected final WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;

    /**
     * For iteration, {@link StreamIterationHead} and {@link StreamIterationTail} used for executing
     * feedback edges do not contain any operators, in which case, {@code mainOperatorWrapper} and
     * {@code tailOperatorWrapper} are null.
     *
     * <p>Usually first operator in the chain is the same as {@link #mainOperatorWrapper}, but
     * that's not the case if there are chained source inputs. In this case, one of the source
     * inputs will be the first operator. For example the following operator chain is possible:
     *
     * <pre>
     * first
     *      \
     *      main (multi-input) -> ... -> tail
     *      /
     * second
     * </pre>
     *
     * <p>Where "first" and "second" (there can be more) are chained source operators. When it comes
     * to things like closing, stat initialisation or state snapshotting, the operator chain is
     * traversed: first, second, main, ..., tail or in reversed order: tail, ..., main, second,
     * first
     */
    @Nullable protected final StreamOperatorWrapper<OUT, OP> mainOperatorWrapper;

    @Nullable protected final StreamOperatorWrapper<?, ?> firstOperatorWrapper;
    @Nullable protected final StreamOperatorWrapper<?, ?> tailOperatorWrapper;

    protected final Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSources;

    protected final int numOperators;

    protected final OperatorEventDispatcherImpl operatorEventDispatcher;

    protected final Closer closer = Closer.create();

    protected final @Nullable FinishedOnRestoreInput finishedOnRestoreInput;

    protected boolean isClosed;






    /**
     * 初始化算子链
     *
     * @param containingTask
     * @param recordWriterDelegate
     */
    public OperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate
    ) {

//        创建发送和接收OperatorEvent的Dispatcher
        this.operatorEventDispatcher = new OperatorEventDispatcherImpl(
                        containingTask.getEnvironment().getUserCodeClassLoader().asClassLoader(),
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway()
                );

        // 获取用户代码类加载器
        final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();

        // 获取任务的配置
        final StreamConfig configuration = containingTask.getConfiguration();

        // 获取StreamTask的StreamOperator工厂
        StreamOperatorFactory<OUT> operatorFactory = configuration.getStreamOperatorFactory(userCodeClassloader);

        // we read the chained configs, and the order of record writer registrations by output name
        // 获取OperatorChain中所有StreamOperator对应的StreamConfig，map的key为jobVertexID，chain后的配置信息【chain】
        // 算子链内部的算子对应的配置信息
        Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

        // create the final output stream writers
        // we iterate through all the out edges from this job vertex and create a stream output
        // 获取顶点排序后的的非chain过的输出列表，相当于StreamEdge【noChain】
        // 每个算子链的最终网络输出
        List<NonChainedOutput> outputsInOrder = configuration.getVertexNonChainedOutputs(userCodeClassloader);

        Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs = new HashMap<>(outputsInOrder.size());

        this.streamOutputs = new RecordWriterOutput<?>[outputsInOrder.size()];

        this.finishedOnRestoreInput = this.isTaskDeployedAsFinished()
                        ? new FinishedOnRestoreInput(streamOutputs, configuration.getInputs(userCodeClassloader).length)
                        : null;


        // from here on, we need to make sure that the output writers are shut down again on failure
        /**
         * https://www.jianshu.com/p/928352c9101d
         * 从现在开始，我们需要确保在发生故障时再次关闭输出写入器。
         *
         * 该句话的意思是，在代码的后续部分，如果出现故障或错误，需要采取措施确保输出写入器被正确关闭。这是为了避免资源泄漏或数据不一致的情况。
         * 具体的实现方式可能因代码的上下文而异，但通常会在异常处理块或错误处理逻辑中添加相应的代码来关闭输出写入器。
         * 关闭输出写入器可以释放相关资源、刷新缓冲区，并确保数据被正确写入目标位置。
         */
        boolean success = false;
        try {
            // 创建链式输出，数据存储在了变量 streamOutputs 和 recordWriterOutputs 内部
            // 用于初始化recordWriterOutputs变量
            // recordWriterOutputs保存了每步操作的StreamEdge和output的对应关系
            // 考虑开发写的物理算子，一个算子链一个输出，算子链内核算子对外输出
            createChainOutputs( // *** 输出对象 ==> recordWriterOutputs 和 streamOutputs ***
                    outputsInOrder,//按顺序排列的输出列表，表示任务中的各个输出。****************
                    recordWriterDelegate,//记录写入器的代理对象，用于实际写入记录。
                    chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
                    containingTask,//包含该链式输出的任务。
                    recordWriterOutputs//记录写入器的输出列表，用于将记录写入到不同的目标。
            );

            // we create the chain of operators and grab the collector that leads into the chain
            // 创建包含所有operatorWrapper的集合，这里是对operator的包装
            List<StreamOperatorWrapper<?, ?>> allOpWrappers = new ArrayList<>(chainedConfigs.size());

            // 创建mainOperator对应的output，输出收集器
            // OperatorChain的入口Operator为mainOperator
            // 这个operator通过ChainingOutput按照数据流向顺序串联了OperatorChain中的所有operator
            // 返回算子链对外输出
            this.mainOperatorOutput = // 返回的是算子链尾节点对应的网络输出对象
                    createOutputCollector(
                            containingTask,//包含该链式输出的任务。
                            configuration,//任务的配置
                            chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
                            userCodeClassloader,// 用户代码类加载器
                            recordWriterOutputs,// 链式输出任务的集合
                            allOpWrappers,// 算子包装类
                            containingTask.getMailboxExecutorFactory() // 任务执行线程工厂
                    );

            // 这里处理上游代码递归返回后剩余的首节点的对象封装工作
            if (operatorFactory != null) {
                // 创建mainOperator和时间服务
                Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                        StreamOperatorFactoryUtil.createOperator(
                                operatorFactory,
                                containingTask,
                                configuration,
                                mainOperatorOutput,
                                operatorEventDispatcher
                        );

                OP mainOperator = mainOperatorAndTimeService.f0;

                // 设置Watermark监控项
                mainOperator
                        .getMetricGroup()
                        .gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, mainOperatorOutput.getWatermarkGauge());

                // 创建mainOperatorWrapper
                this.mainOperatorWrapper = // 这个对象在map-filter 链中指的是map对应的算子封装对象
                        createOperatorWrapper(
                                mainOperator,
                                containingTask,
                                configuration,
                                mainOperatorAndTimeService.f1,
                                true
                        );

                // add main operator to end of chain
                // 将mainOperatorWrapper添加到chain的最后
                allOpWrappers.add(mainOperatorWrapper);

                // createOutputCollector方法将各个operator包装到operatorWrapper中
                // 按照数据流相反的顺序加入到allOpWrappers集合
                // 所以，尾部的operatorWrapper就是index为0的元素
                // 这个对象在map-filter 链中指的是filter对应的算子封装对象
                this.tailOperatorWrapper = allOpWrappers.get(0);

            } else {
                // 如果OperatorFactory为null
                checkState(allOpWrappers.size() == 0);
                this.mainOperatorWrapper = null;
                this.tailOperatorWrapper = null;
            }


            // 创建chain的数据源，如果该算子链不是source算子，那么返回空集合，什么都不做
            // 这里应该只考虑多源的情况
            this.chainedSources =
                    createChainedSources(
                            containingTask,
                            configuration.getInputs(userCodeClassloader), // 首节点配置获取收入信息
                            chainedConfigs,
                            userCodeClassloader,
                            allOpWrappers // 本chain中的所有算子封装集合
                    );

            this.numOperators = allOpWrappers.size();

            // 将所有的StreamOperatorWrapper按照从上游到下游的顺序，形成双向链表
            firstOperatorWrapper = linkOperatorWrappers(allOpWrappers);

            success = true;

        } finally {
            // make sure we clean up after ourselves in case of a failure after acquiring
            // the first resources
            if (!success) {
                for (int i = 0; i < streamOutputs.length; i++) {
                    if (streamOutputs[i] != null) {
                        streamOutputs[i].close();
                    }
                    streamOutputs[i] = null;
                }
            }
        }
    }







    @VisibleForTesting
    OperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper
    ) {
        this.streamOutputs = streamOutputs;
        this.finishedOnRestoreInput = null;
        this.mainOperatorOutput = checkNotNull(mainOperatorOutput);
        this.operatorEventDispatcher = null;

        checkState(allOperatorWrappers != null && allOperatorWrappers.size() > 0);
        this.mainOperatorWrapper = checkNotNull(mainOperatorWrapper);
        this.tailOperatorWrapper = allOperatorWrappers.get(0);
        this.numOperators = allOperatorWrappers.size();
        this.chainedSources = Collections.emptyMap();

        firstOperatorWrapper = linkOperatorWrappers(allOperatorWrappers);
    }

    public abstract boolean isTaskDeployedAsFinished();

    public abstract void dispatchOperatorEvent(
            OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException;

    public abstract void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Ends the main operator input specified by {@code inputId}).
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    public abstract void endInput(int inputId) throws Exception;

    /**
     * Initialize state and open all operators in the chain from <b>tail to heads</b>, contrary to
     * {@link StreamOperator#close()} which happens <b>heads to tail</b> (see {@link
     * #finishOperators(StreamTaskActionExecutor, StopMode)}).
     */
    public abstract void initializeStateAndOpenOperators(StreamTaskStateInitializer streamTaskStateInitializer) throws Exception;

    /**
     * Closes all operators in a chain effect way. Closing happens from <b>heads to tail</b>
     * operator in the chain, contrary to {@link StreamOperator#open()} which happens <b>tail to
     * heads</b> (see {@link #initializeStateAndOpenOperators(StreamTaskStateInitializer)}).
     */
    public abstract void finishOperators(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception;

    public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    public abstract void notifyCheckpointAborted(long checkpointId) throws Exception;

    public abstract void notifyCheckpointSubsumed(long checkpointId) throws Exception;

    public abstract void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception;

    public OperatorEventDispatcher getOperatorEventDispatcher() {
        return operatorEventDispatcher;
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.broadcastEvent(event, isPriorityEvent);
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.alignedBarrierTimeout(checkpointId);
        }
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.abortCheckpoint(checkpointId, cause);
        }
    }

    /**
     * Execute {@link StreamOperator#close()} of each operator in the chain of this {@link
     * StreamTask}. Closing happens from <b>tail to head</b> operator in the chain.
     */
    public void closeAllOperators() throws Exception {
        isClosed = true;
    }

    public RecordWriterOutput<?>[] getStreamOutputs() {
        return streamOutputs;
    }

    /** Returns an {@link Iterable} which traverses all operators in forward topological order. */
    @VisibleForTesting
    public Iterable<StreamOperatorWrapper<?, ?>> getAllOperators() {
        return getAllOperators(false);
    }

    /**
     * Returns an {@link Iterable} which traverses all operators in forward or reverse topological
     * order.
     */
    protected Iterable<StreamOperatorWrapper<?, ?>> getAllOperators(boolean reverse) {
        return reverse
                ? new StreamOperatorWrapper.ReadIterator(tailOperatorWrapper, true)
                : new StreamOperatorWrapper.ReadIterator(mainOperatorWrapper, false);
    }

    public Input getFinishedOnRestoreInputOrDefault(Input defaultInput) {
        return finishedOnRestoreInput == null ? defaultInput : finishedOnRestoreInput;
    }

    public int getNumberOfOperators() {
        return numOperators;
    }

    public WatermarkGaugeExposingOutput<StreamRecord<OUT>> getMainOperatorOutput() {
        return mainOperatorOutput;
    }

    public ChainedSource getChainedSource(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput);
    }

    public List<Output<StreamRecord<?>>> getChainedSourceOutputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceOutput)
                .collect(Collectors.toList());
    }

    public StreamTaskSourceInput<?> getSourceTaskInput(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput).getSourceTaskInput();
    }

    public List<StreamTaskSourceInput<?>> getSourceTaskInputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceTaskInput)
                .collect(Collectors.toList());
    }

    /**
     * This method should be called before finishing the record emission, to make sure any data that
     * is still buffered will be sent. It also ensures that all data sending related exceptions are
     * recognized.
     *
     * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
     */
    public void flushOutputs() throws IOException {
        for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
            streamOutput.flush();
        }
    }

    /**
     * This method releases all resources of the record writer output. It stops the output flushing
     * thread (if there is one) and releases all buffers currently held by the output serializers.
     *
     * <p>This method should never fail.
     */
    public void close() throws IOException {
        closer.close();
    }

    @Nullable
    public OP getMainOperator() {
        return (mainOperatorWrapper == null) ? null : mainOperatorWrapper.getStreamOperator();
    }

    @Nullable
    protected StreamOperator<?> getTailOperator() {
        return (tailOperatorWrapper == null) ? null : tailOperatorWrapper.getStreamOperator();
    }

    protected void snapshotChannelStates(
            StreamOperator<?> op,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            OperatorSnapshotFutures snapshotInProgress
    ) {
        if (op == getMainOperator()) {
            // 写入状态结果
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of)
            );
        }
        if (op == getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of)
            );
        }
    }



    public boolean isClosed() {
        return isClosed;
    }

    /** Wrapper class to access the chained sources and their's outputs. */
    public static class ChainedSource {
        private final WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput;
        private final StreamTaskSourceInput<?> sourceTaskInput;

        public ChainedSource(
                WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput,
                StreamTaskSourceInput<?> sourceTaskInput
        ) {
            this.chainedSourceOutput = chainedSourceOutput;
            this.sourceTaskInput = sourceTaskInput;
        }

        public WatermarkGaugeExposingOutput<StreamRecord<?>> getSourceOutput() {
            return chainedSourceOutput;
        }

        public StreamTaskSourceInput<?> getSourceTaskInput() {
            return sourceTaskInput;
        }
    }

    // ------------------------------------------------------------------------
    //  initialization utilities
    // ------------------------------------------------------------------------

    /**
     *                     outputsInOrder,//按顺序排列的输出列表，表示任务中的各个输出。
     *                     recordWriterDelegate,//记录写入器的代理对象，用于实际写入记录。
     *                     chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
     *                     containingTask,//包含该链式输出的任务。
     *                     recordWriterOutputs//记录写入器的输出列表，用于将记录写入到不同的目标。
     * @param outputsInOrder
     * @param recordWriterDelegate
     * @param chainedConfigs
     * @param containingTask
     * @param recordWriterOutputs
     */
    private void createChainOutputs(
            List<NonChainedOutput> outputsInOrder,//按顺序排列的输出列表，表示任务中的各个输出。
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate,//记录写入器的代理对象，用于实际写入记录。
            Map<Integer, StreamConfig> chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
            StreamTask<OUT, OP> containingTask,//包含该链式输出的任务。
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs//记录写入器的输出列表，用于将记录写入到不同的目标。
    ) {

        for (int i = 0; i < outputsInOrder.size(); ++i) {
            NonChainedOutput output = outputsInOrder.get(i);

            RecordWriterOutput<?> recordWriterOutput =
                    createStreamOutput(
                            recordWriterDelegate.getRecordWriter(i),//记录写入器的代理对象
                            output,// 任务的输出
                            chainedConfigs.get(output.getSourceNodeId()), // 获取输出对应的配置信息
                            containingTask.getEnvironment() // 环境变量
                    );

            this.streamOutputs[i] = recordWriterOutput;
            recordWriterOutputs.put(output.getDataSetId(), recordWriterOutput);
        }
    }

    private RecordWriterOutput<OUT> createStreamOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,//记录写入器的代理对象
            NonChainedOutput streamOutput,// 任务的输出
            StreamConfig upStreamConfig,// 获取输出对应的配置信息
            Environment taskEnvironment// 环境变量
    ) {

        OutputTag sideOutputTag = streamOutput.getOutputTag(); // OutputTag, return null if not sideOutput

        TypeSerializer outSerializer;

        if (streamOutput.getOutputTag() != null) {
            // side output
            outSerializer =
                    upStreamConfig.getTypeSerializerSideOut(
                            streamOutput.getOutputTag(),
                            taskEnvironment.getUserCodeClassLoader().asClassLoader()
                    );
        } else {
            // main output
            outSerializer =
                    upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserCodeClassLoader().asClassLoader());
        }

        return closer.register(
                new RecordWriterOutput<OUT>(
                        recordWriter,
                        outSerializer,
                        sideOutputTag,
                        streamOutput.supportsUnalignedCheckpoints()
                )
        );
    }


    /**
     * 该方法用于创建chained数据源。
     *
     * @param containingTask
     * @param configuredInputs
     * @param chainedConfigs
     * @param userCodeClassloader
     * @param allOpWrappers
     * @return
     */
    @SuppressWarnings("rawtypes")
    private Map<StreamConfig.SourceInputConfig, ChainedSource> createChainedSources(
            StreamTask<OUT, OP> containingTask, // 算子对象
            StreamConfig.InputConfig[] configuredInputs, // 首节点对应的输入源信息
            Map<Integer, StreamConfig> chainedConfigs, // 算子节点配置集合
            ClassLoader userCodeClassloader, // 用户代码类加载器
            List<StreamOperatorWrapper<?, ?>> allOpWrappers // 算子链所有算子集合，已经逐个处理好了
    ) {

        // 如果所有的configuredInputs都不是SourceInputConfig类型，返回空map
        if (Arrays.stream(configuredInputs).noneMatch(input -> input instanceof StreamConfig.SourceInputConfig)) {
            return Collections.emptyMap();
        }

        // chained 数据源只适用于多个输入的StreamOperator
        checkState(
                mainOperatorWrapper.getStreamOperator() instanceof MultipleInputStreamOperator,
                "Creating chained input is only supported with MultipleInputStreamOperator and MultipleInputStreamTask");

//        创建输入源集合
        Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSourceInputs = new HashMap<>();
        MultipleInputStreamOperator<?> multipleInputOperator = (MultipleInputStreamOperator<?>) mainOperatorWrapper.getStreamOperator();

        // 获取它所有的Input
        List<Input> operatorInputs = multipleInputOperator.getInputs();

        // 计算InputGate的Index，为所有InputGate的index最大值加1
        int sourceInputGateIndex =
                Arrays.stream(containingTask.getEnvironment().getAllInputGates())
                                .mapToInt(IndexedInputGate::getInputGateIndex)
                                .max()
                                .orElse(-1)
                        + 1;

        // 遍历每个Input
        for (int inputId = 0; inputId < configuredInputs.length; inputId++) {

            // 排除掉所有不是SourceInputConfig类型的情况
            if (!(configuredInputs[inputId] instanceof StreamConfig.SourceInputConfig)) {
                continue;
            }

            StreamConfig.SourceInputConfig sourceInput = (StreamConfig.SourceInputConfig) configuredInputs[inputId];
            int sourceEdgeId = sourceInput.getInputEdge().getSourceId();

            // 根据input edge获取sourceInputConfig
            StreamConfig sourceInputConfig = chainedConfigs.get(sourceEdgeId);
            OutputTag outputTag = sourceInput.getInputEdge().getOutputTag();

            // 创建链式的数据源output
            // 目前只支持Object Reuse开启
            // 实际返回的类型为ChainingOutput
            WatermarkGaugeExposingOutput chainedSourceOutput =
                    createChainedSourceOutput(
                            containingTask,
                            sourceInputConfig,
                            userCodeClassloader,
                            getFinishedOnRestoreInputOrDefault(operatorInputs.get(inputId)),
                            multipleInputOperator.getMetricGroup(),
                            outputTag
                    );

            // 创建数据源operator
            // createOperator前面分析过，不再赘述
            SourceOperator<?, ?> sourceOperator = (SourceOperator<?, ?>) createOperator(
                                    containingTask,
                                    sourceInputConfig,
                                    userCodeClassloader,
                                    (WatermarkGaugeExposingOutput<StreamRecord<OUT>>) chainedSourceOutput,
                                    allOpWrappers,
                                    true
                            );

            // 放入chainedSourceInputs中
            chainedSourceInputs.put(
                    sourceInput,
                    new ChainedSource(
                            chainedSourceOutput,
                            this.isTaskDeployedAsFinished() ?
                                    new StreamTaskFinishedOnRestoreSourceInput<>(sourceOperator, sourceInputGateIndex++, inputId)
                                    : new StreamTaskSourceInput<>(sourceOperator, sourceInputGateIndex++, inputId)
                    )
            );
        }

        return chainedSourceInputs;
    }






    @SuppressWarnings({"rawtypes", "unchecked"})
    private WatermarkGaugeExposingOutput<StreamRecord> createChainedSourceOutput(
            StreamTask<?, OP> containingTask,
            StreamConfig sourceInputConfig,
            ClassLoader userCodeClassloader,
            Input input,
            OperatorMetricGroup metricGroup,
            OutputTag outputTag
    ) {

        WatermarkGaugeExposingOutput<StreamRecord> chainedSourceOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            chainedSourceOutput = new ChainingOutput(input, metricGroup, outputTag);
        } else {
            TypeSerializer<?> inSerializer = sourceInputConfig.getTypeSerializerOut(userCodeClassloader);
            chainedSourceOutput = new CopyingChainingOutput(input, inSerializer, metricGroup, outputTag);
        }
        /**
         * Chained sources are closed when {@link
         * org.apache.flink.streaming.runtime.io.StreamTaskSourceInput} are being closed.
         */
        return closer.register(chainedSourceOutput);
    }


    /**
     *                             containingTask,//包含该链式输出的任务。
     *                             configuration,//任务的配置
     *                             chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
     *                             userCodeClassloader,// 用户代码类加载器
     *                             recordWriterOutputs,// 链式输出任务的集合
     *                             allOpWrappers,// 算子包装类
     *                             containingTask.getMailboxExecutorFactory() // 任务执行线程工厂
     *
     * @param containingTask
     * @param operatorConfig
     * @param chainedConfigs
     * @param userCodeClassloader
     * @param recordWriterOutputs
     * @param allOperatorWrappers
     * @param mailboxExecutorFactory
     * @return
     * @param <T>
     */
    private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
            StreamTask<?, ?> containingTask,//包含该链式输出的任务。
            StreamConfig operatorConfig,//任务的配置
            Map<Integer, StreamConfig> chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
            ClassLoader userCodeClassloader,// 用户代码类加载器
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,// 链式输出任务的集合
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,// 算子包装类
            MailboxExecutorFactory mailboxExecutorFactory// 任务执行线程工厂
    ) {
        // 定义输出集合
        List<WatermarkGaugeExposingOutput<StreamRecord<T>>> allOutputs = new ArrayList<>(4);

        // create collectors for the network outputs
        // 遍历非链式【nochain】输出，非链式的输出需要走网络连接
        // 因此生成的Output类型为RecordWriterOutput
        // 如果是算子链的输出算子，那么就走下边分支，否则就不走下边分支，例如map-filter算子，遍历map算子因为不是最终输出所以不走下边分支
        for (NonChainedOutput streamOutput : operatorConfig.getOperatorNonChainedOutputs(userCodeClassloader)) {
            // 从上一步createChainOutputs方法返回的recordWriterOutputs中获取对应的output
            @SuppressWarnings("unchecked")
            RecordWriterOutput<T> recordWriterOutput = (RecordWriterOutput<T>) recordWriterOutputs.get(streamOutput.getDataSetId());

//            先进去的是头，后进去的是尾
            allOutputs.add(recordWriterOutput);
        }

        // Create collectors for the chained outputs
        // 获取该Operator对应的所有chained内的输出
        // 如果这个Operator具有多个chained的下游，这里会获取到多个outEdge
        // 如果不是算子链的最后一个算子，则走这个分支，否则不走这个分支，例如map-filter算子，遍历map算子因为不是最终输出所以走下边分支
        for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {

            int outputId = outputEdge.getTargetId(); // 输出ID
            // 获取这个输出对应的StreamConfig
            // map-filter算子指的是filter的输出配置，即当前算子的下一个算子的配置信息
            StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

            // 根据输出生成streamOutput，为WatermarkGaugeExposingOutput类型
            // WatermarkGaugeExposingOutput包装了Output和一个监控watermark的仪表盘
            // 如果存在可以chain的operator，需要递归调用，将下游与上游链接起来
            WatermarkGaugeExposingOutput<StreamRecord<T>> output = // 这里的output指的是算子链内部算子间的输出，即本地输出
                    /**
                     * 然后需要分析createOperatorChain方法。它将OperatorChain中所有的Operator包装为StreamOperatorWrapper类型，按照数据流反方向存入allOperatorWrappers集合。
                     * 根据operator的顺序，依次生成ChainingOutput，将各个operator数据流串联起来。
                     *
                     */
                    createOperatorChain(
                            containingTask,//包含该链式输出的任务。
                            chainedOpConfig,// 本次循环输出节点【即下一个节点的配置信息】的配置
                            chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
                            userCodeClassloader,// 用户代码类加载器
                            recordWriterOutputs,// 链式输出任务的集合
                            allOperatorWrappers,// 算子包装类
                            outputEdge.getOutputTag(),// 下一个算子标识
                            mailboxExecutorFactory// 任务执行线程工厂
                    );

            // 将其加入allOutputs集合中
            allOutputs.add(output);
        }

        if (allOutputs.size() == 1) {
            return allOutputs.get(0);
        } else {
            // send to N outputs. Note that this includes the special case
            // of sending to zero outputs
            // 如果有多个输出，将allOutputs转换为Output类型数组
            @SuppressWarnings({"unchecked"})
            Output<StreamRecord<T>>[] asArray = new Output[allOutputs.size()];
            for (int i = 0; i < allOutputs.size(); i++) {
                asArray[i] = allOutputs.get(i);
            }

            // This is the inverse of creating the normal ChainingOutput.
            // If the chaining output does not copy we need to copy in the broadcast output,
            // otherwise multi-chaining would not work correctly.
            // 根据配置中对象是否可重用，创建不同的OutputCollector
            // 在StreamRecord发往下游的时候实际发送的是StreamRecord的浅拷贝
            // 避免使用深拷贝，从而提高性能，但是需要注意如果开启ObjectReuse
            // 避免在下游改变流数据元素的值，否则会出现线程安全问题
            if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
                return closer.register(new CopyingBroadcastingOutputCollector<>(asArray));
            } else {
                return closer.register(new BroadcastingOutputCollector<>(asArray));
            }
        }
    }




    /**
     * Recursively create chain of operators that starts from the given {@param operatorConfig}.
     * Operators are created tail to head and wrapped into an {@link WatermarkGaugeExposingOutput}.
     *
     *                             containingTask,//包含该链式输出的任务。
     *                             chainedOpConfig,// 输出节点的配置
     *                             chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
     *                             userCodeClassloader,// 用户代码类加载器
     *                             recordWriterOutputs,// 链式输出任务的集合
     *                             allOperatorWrappers,// 算子包装类
     *                             outputEdge.getOutputTag(),// 输出标识
     *                             mailboxExecutorFactory// 任务执行线程工厂
     */
    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createOperatorChain( // map算子内部进入到这里的
            StreamTask<OUT, ?> containingTask,//包含该链式输出的任务。
            StreamConfig operatorConfig,// 下一个节点的配置
            Map<Integer, StreamConfig> chainedConfigs,//被链接的配置列表，可能是与输出相关的其他配置信息。
            ClassLoader userCodeClassloader,// 用户代码类加载器
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,// 链式输出任务的集合
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,// 算子包装类
            OutputTag<IN> outputTag,// 输出标识
            MailboxExecutorFactory mailboxExecutorFactory// 任务执行线程工厂
    ) {
        // create the output that the operator writes to first. this may recursively create more
        // operators
        // 这里的operatorConfig为前一个方法中每次遍历的chainedOpConfig
        // 这里存在一个递归调用，将下游outEdge对应的StreamConfig作为参数，再次调用createOutputCollector
        // 最终的效果为上游operator的output指向下游operator，实现了chain，即链式调用
        // 最先返回的是最下游的output
        // operator的output按照从下游到上游的顺序，依次被包装为WatermarkGaugeExposingOutput
        WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput =
                createOutputCollector( // 递归调用
                        containingTask,
                        operatorConfig, // 输出配置
                        chainedConfigs,
                        userCodeClassloader,
                        recordWriterOutputs,
                        allOperatorWrappers,
                        mailboxExecutorFactory
                );

        // 创建链式operator
        // 参数中使用上一步生成的operator output
        // 输出算子才会创建这个输出
        // 将map-filter算子遍历到filter算子进行算子创建操作
        // 这里首先会创建尾节点的operator算子，从后向前递归返回
        OneInputStreamOperator<IN, OUT> chainedOperator =
                createOperator(
                        containingTask,//包含该链式输出的任务。
                        operatorConfig,// 输出配置
                        userCodeClassloader,// 用户代码类加载器
                        chainedOperatorOutput,// 本次创建的operator的output对象
                        allOperatorWrappers,// 算子包装类
                        false // 是否头节点 => 从后向前
                );

        // 将operator包装到output中并返回;
        // 它将operator包装到Output中。output的类型为ChainingOutput
        return wrapOperatorIntoOutput(chainedOperator, containingTask, operatorConfig, userCodeClassloader, outputTag);
    }




    /**
     * Create and return a single operator from the given {@param operatorConfig} that will be
     * producing records to the {@param output}.
     *
     *                         containingTask,//包含该链式输出的任务。
     *                         operatorConfig,// 输出配置
     *                         userCodeClassloader,// 用户代码类加载器
     *                         chainedOperatorOutput,// 本次创建的operator的output对象
     *                         allOperatorWrappers,// 算子包装类
     *                         false // 是否头节点
     */
    private <OUT, OP extends StreamOperator<OUT>> OP createOperator(
            StreamTask<OUT, ?> containingTask,//包含该链式输出的任务。
            StreamConfig operatorConfig,// 输出配置
            ClassLoader userCodeClassloader,// 用户代码类加载器
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> output,// 本次创建的operator的output对象
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,// 算子包装类
            boolean isHead// 是否头节点
    ) {

        // now create the operator and give it the output collector to write its output to
        // 使用StreamOperatorFactory创建出一个StreamOperator，使用指定的output
        Tuple2<OP, Optional<ProcessingTimeService>> chainedOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorConfig.getStreamOperatorFactory(userCodeClassloader),// 算子工厂
                        containingTask,//包含该链式输出的任务。
                        operatorConfig,// 输出配置
                        output,// 上一个节点创建的operator的output对象
                        operatorEventDispatcher
                );

        // 获取创建的operator
        OP chainedOperator = chainedOperatorAndTimeService.f0;
        // 使用StreamOperatorWrapper包装此新创建的operator
        // StreamOperatorWrapper是operator在chaining时执行专用的封装类型，后面分析
        // 由于是递归调用，最先执行到这里的是最下游的算子
        // 因此allOperatorWrappers保存的顺序实际上是operator按照数据流向反向排列
        allOperatorWrappers.add(
                // 该方法使用StreamOperatorWrapper将StreamOperator包装起来
                createOperatorWrapper(
                        chainedOperator,
                        containingTask,
                        operatorConfig,
                        chainedOperatorAndTimeService.f1,
                        isHead
                )
        );

        // 添加一个watermark监控用仪表盘
        chainedOperator
                .getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                        output.getWatermarkGauge()::getValue);

        return chainedOperator;
    }





    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> wrapOperatorIntoOutput(
            OneInputStreamOperator<IN, OUT> operator,
            StreamTask<OUT, ?> containingTask,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            OutputTag<IN> outputTag
    ) {

        WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;

        // 如果开启了对象重用，创建ChainingOutput
        // 具体ChainingOutput相关内容在接下来章节分析
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
//            ChainingOutput实现了把上游operator的输出作为下一个operator的输入。创建ChainingOutput时需要传入下游operator，保存到input属性中。
            currentOperatorOutput = new ChainingOutput<>(operator, outputTag);
        } else {
            // 否则创建CopyingChainingOutput
            // 传递StreamRecord时会进行深拷贝
            // ChainingOutput还有一个子类叫做CopyingChainingOutput。它重写了pushToOperator方法，在数据发送往下游operator之前会创建一个深拷贝。如果启用了Object重用（containingTask.getExecutionConfig().isObjectReuseEnabled()返回true），使用ChainingOutput，否则使用CopyingChainingOutput。
            TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
            currentOperatorOutput = new CopyingChainingOutput<>(operator, inSerializer, outputTag);
        }

        // wrap watermark gauges since registered metrics must be unique
        // 创建一个watermark监控仪表
        operator.getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_INPUT_WATERMARK,
                        currentOperatorOutput.getWatermarkGauge()::getValue
                );

        return closer.register(currentOperatorOutput);
    }

    /**
     * Links operator wrappers in forward topological order.
     *
     * allOperatorWrappers：按照算子顺序从后到前存储的，比如map-filter算子链存储的事[filter，map]集合
     *
     * @param allOperatorWrappers is an operator wrapper list of reverse topological order
     */
    private StreamOperatorWrapper<?, ?> linkOperatorWrappers(List<StreamOperatorWrapper<?, ?>> allOperatorWrappers) {
        StreamOperatorWrapper<?, ?> previous = null;
//        从后向前遍历
        for (StreamOperatorWrapper<?, ?> current : allOperatorWrappers) {
            if (previous != null) {
                previous.setPrevious(current);
            }
            current.setNext(previous);
            previous = current;
        }
        return previous;
    }

    private <T, P extends StreamOperator<T>> StreamOperatorWrapper<T, P> createOperatorWrapper(
            P operator,
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Optional<ProcessingTimeService> processingTimeService,
            boolean isHead
    ) {
        return new StreamOperatorWrapper<>(
                operator,
                processingTimeService,
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(operatorConfig.getChainIndex()),
                isHead
        );
    }

    protected void sendAcknowledgeCheckpointEvent(long checkpointId) {
        if (operatorEventDispatcher == null) {
            return;
        }

        operatorEventDispatcher
                .getRegisteredOperators()
                .forEach(
                        x ->
                                operatorEventDispatcher
                                        .getOperatorEventGateway(x)
                                        .sendEventToCoordinator(
                                                new AcknowledgeCheckpointEvent(checkpointId)
                                        )
                );
    }
}
