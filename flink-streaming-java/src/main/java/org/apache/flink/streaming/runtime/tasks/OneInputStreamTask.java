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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInputFactory;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.flink.shaded.curator5.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamTask} for executing a {@link OneInputStreamOperator}. */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;

    private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    public OneInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the time provider) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param env The task environment for this task.
     * @param timeProvider Optionally, a specific time provider to use.
     */
    @VisibleForTesting
    public OneInputStreamTask(Environment env, @Nullable TimerService timeProvider)
            throws Exception {
        super(env, timeProvider);
    }

    /**
     * OneInputStreamTask 初始化
     *
     * @throws Exception
     */
    @Override
    public void init() throws Exception {

        // 获取配置和输入源数量
        StreamConfig configuration = getConfiguration();
        int numberOfInputs = configuration.getNumberOfNetworkInputs();

//        存在输入源
        if (numberOfInputs > 0) {
            /**
             * 创建检查点inputGate
             *
             * CheckpointedInputGate 是 Flink 中用于从输入通道读取数据并支持检查点的输入门（Input Gate）。
             * 输入门是 Flink 数据流任务中的一个重要组件，用于接收来自上游任务的数据，并在执行检查点时将数据持久化到状态后端，以支持故障恢复和一致性保证。
             */
            CheckpointedInputGate inputGate = createCheckpointedInputGate();

            Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);

            DataOutput<IN> output = createDataOutput(numRecordsIn);
            StreamTaskInput<IN> input = createTaskInput(inputGate);

            StreamConfig.InputConfig[] inputConfigs = configuration.getInputs(getUserCodeClassLoader());
            StreamConfig.InputConfig inputConfig = inputConfigs[0];
            if (requiresSorting(inputConfig)) {
                checkState(
                        !configuration.isCheckpointingEnabled(),
                        "Checkpointing is not allowed with sorted inputs.");

                input = wrapWithSorted(input);
            }

            getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .reuseRecordsInputCounter(numRecordsIn);

            inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);

        }

        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);
    }




    @Override
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        return new SortingDataInput<>(
                input,
                configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
                configuration.getStateKeySerializer(userCodeClassLoader),
                configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
                getEnvironment().getMemoryManager(),
                getEnvironment().getIOManager(),
                getExecutionConfig().isObjectReuseEnabled(),
                configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader),
                getJobConfiguration(),
                this,
                getExecutionConfig()
        );
    }

    @SuppressWarnings("unchecked")
    private CheckpointedInputGate createCheckpointedInputGate() {

//        获取所有的inputgate对象，并且保存为数组
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        /**
         * 这段代码创建了一个 CheckpointBarrierHandler 对象，并将其赋值给变量 checkpointBarrierHandler。下面是对代码的解释：
         *
         * InputProcessorUtil.createCheckpointBarrierHandler(...)：调用 InputProcessorUtil 工具类的静态方法 createCheckpointBarrierHandler
         * 来创建一个 CheckpointBarrierHandler 对象。该方法可能包含了创建和配置 CheckpointBarrierHandler 对象所需的逻辑。
         * this：作为参数传递给方法的当前对象，可能是指当前的任务实例。
         * configuration：作为参数传递给方法的配置对象，用于配置 CheckpointBarrierHandler。
         * getCheckpointCoordinator()：调用一个方法（getCheckpointCoordinator()）来获取检查点协调器（Checkpoint Coordinator）的实例。
         * getTaskNameWithSubtaskAndId()：调用一个方法（getTaskNameWithSubtaskAndId()）来获取任务的名称、子任务和标识符的组合。
         * new List[] {Arrays.asList(inputGates)}：作为参数传递给方法的输入门（InputGate）对象的列表。可能是一个包含输入门的数组或列表。
         * Collections.emptyList()：作为参数传递给方法的空列表，表示不使用额外的约束条件。
         * mainMailboxExecutor：作为参数传递给方法的邮箱执行器（MailboxExecutor）对象。该邮箱执行器用于异步执行 CheckpointBarrierHandler 中的操作。
         * systemTimerService：作为参数传递给方法的系统定时器服务（SystemTimerService）对象，用于计时器相关的操作。
         * 通过调用 InputProcessorUtil.createCheckpointBarrierHandler(...) 方法，可以创建一个经过配置的 CheckpointBarrierHandler 对象，该对象用于处理检查点相关的任务操作。将创建的 CheckpointBarrierHandler 对象赋值给 checkpointBarrierHandler 变量后，可以在后续的代码中使用该对象来处理检查点相关的任务操作，例如处理检查点屏障和状态快照等。
         *
         * 需要注意的是，具体的 createCheckpointBarrierHandler(...) 方法的实现和使用细节取决于上下文和应用程序的需求。要全面理解代码的含义和功能，需要查看 InputProcessorUtil.createCheckpointBarrierHandler(...) 方法的实现，并了解 CheckpointBarrierHandler 类的定义和使用方式。
         */
        checkpointBarrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[] {Arrays.asList(inputGates)},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService
                );

//        并行创建检查点inputgate
        CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[] {Arrays.asList(inputGates)},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration
                );

        return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
    }



    private DataOutput<IN> createDataOutput(Counter numRecordsIn) {
        return new StreamTaskNetworkOutput<>(
                operatorChain.getFinishedOnRestoreInputOrDefault(mainOperator),
                inputWatermarkGauge,
                numRecordsIn
        );
    }



    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {

        int numberOfInputChannels = inputGate.getNumberOfInputChannels();
        StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels);
        TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());

        return StreamTaskNetworkInputFactory.create(
                inputGate,
                inSerializer,
                getEnvironment().getIOManager(),
                statusWatermarkValve,
                0,
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gateIndex -> configuration
                                .getInPhysicalEdges(getUserCodeClassLoader())
                                .get(gateIndex)
                                .getPartitioner(),
                getEnvironment().getTaskInfo()
        );
    }




    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in one input processor.
     */
    private static class StreamTaskNetworkOutput<IN> implements DataOutput<IN> {

        private final Input<IN> operator;

        private final WatermarkGauge watermarkGauge;
        private final Counter numRecordsIn;

        private StreamTaskNetworkOutput(Input<IN> operator, WatermarkGauge watermarkGauge, Counter numRecordsIn) {

            this.operator = checkNotNull(operator);
            this.watermarkGauge = checkNotNull(watermarkGauge);
            this.numRecordsIn = checkNotNull(numRecordsIn);
        }

        @Override
        public void emitRecord(StreamRecord<IN> record) throws Exception {
            numRecordsIn.inc();
            operator.setKeyContextElement(record);
            operator.processElement(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            operator.processWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            operator.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            operator.processLatencyMarker(latencyMarker);
        }
    }
}
