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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in a
 * synchronized block that locks on the lock Object. Also, the modification of the state and the
 * emission of elements must happen in the same block of code that is protected by the synchronized
 * block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<
                OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private enum FinishingReason {
        END_OF_DATA(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_DRAIN(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_NO_DRAIN(StopMode.NO_DRAIN);

        private final StopMode stopMode;

        FinishingReason(StopMode stopMode) {
            this.stopMode = stopMode;
        }

        StopMode toStopMode() {
            return this.stopMode;
        }
    }

    /**
     * Indicates whether this Task was purposefully finished, in this case we want to ignore
     * exceptions thrown after finishing, to ensure shutdown works smoothly.
     *
     * <p>Moreover we differentiate drain and no drain cases to see if we need to call finish() on
     * the operators.
     */
    private volatile FinishingReason finishingReason = FinishingReason.END_OF_DATA;

    public SourceStreamTask(Environment env) throws Exception {
        this(env, new Object());
    }

    private SourceStreamTask(Environment env, Object lock) throws Exception {
        super(
                env,
                null,
                FatalExitExceptionHandler.INSTANCE,
                StreamTaskActionExecutor.synchronizedExecutor(lock)
        );

        this.lock = Preconditions.checkNotNull(lock);

        // 用来接收数据的线程
        this.sourceThread = new LegacySourceFunctionThread();

        getEnvironment().getMetricGroup().getIOMetricGroup().setEnableBusyTime(false);
    }

    /**
     * SourceStreamTask 状态初始化工作
     */
    @Override
    protected void init() {
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        // 获取用户源定义方法
        SourceFunction<?> source = mainOperator.getUserFunction();

        /**
         * 在 Apache Flink 中，ExternallyInducedSource 是一个表示外部触发源的类。它是 Flink 数据流编程模型中的一个组件，用于从外部系统或事件触发器异步地生成数据流。
         *
         * ExternallyInducedSource 类可以用于实现自定义的数据源，以便根据外部事件或触发器的到来，生成相应的数据记录并将其发送到 Flink 数据流中进行处理。
         *
         * 通常情况下，Flink 中的数据源是以连续流（continuous stream）的方式生成数据，也就是源源不断地产生数据。
         * 但是，对于一些场景，我们可能需要根据外部事件来触发数据的生成，例如从消息队列中接收到新的消息时才产生数据。
         * 这种情况下，可以使用 ExternallyInducedSource 来实现这样的外部触发数据源。
         *
         * 通过继承 ExternallyInducedSource 类并实现其抽象方法，可以编写自定义的外部触发源。这样，当外部事件或触发器到达时，可以在 run() 方法中生成相应的数据记录并将其发送到 Flink 数据流中。
         *
         * 需要注意的是，具体的使用和实现细节可能会根据应用程序的需求和上下文而有所不同。要了解更多关于 ExternallyInducedSource 类的详细信息，可以查阅 Flink 的官方文档和相关资源。
         */
        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook =
                    new ExternallyInducedSource.CheckpointTrigger() {

                        @Override
                        public void triggerCheckpoint(long checkpointId) throws FlinkException {
                            // TODO - we need to see how to derive those. We should probably not
                            // encode this in the
                            // TODO -   source's trigger message, but do a handshake in this task
                            // between the trigger
                            // TODO -   message from the master, and the source's trigger
                            // notification
                            final CheckpointOptions checkpointOptions =
                                    CheckpointOptions.forConfig(
                                            CheckpointType.CHECKPOINT,
                                            CheckpointStorageLocationReference.getDefault(),
                                            configuration.isExactlyOnceCheckpointMode(),
                                            configuration.isUnalignedCheckpointsEnabled(),
                                            configuration.getAlignedCheckpointTimeout().toMillis()
                                    );
                            final long timestamp = System.currentTimeMillis();

                            final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp, timestamp);

                            try {
                                SourceStreamTask.super
                                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions)
                                        .get();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new FlinkException(e.getMessage(), e);
                            }
                        }
                    };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }

        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos
                );

        recordWriter.setMaxOverdraftBuffersPerGate(0);
    }




    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        operatorChain.getMainOperatorOutput().emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void cleanUpInternal() {
        // does not hold any resources, so no cleanup needed
    }




    /**
     * 用来处理数据源的processInput()方法
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception
     */
    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

//        阻塞执行，因为代码执行到这步后有些对象还没有完全准备好
        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but
        // blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop,
        // not in steps).
        sourceThread.setTaskDescription(getName());

        sourceThread.start();

        sourceThread
                .getCompletionFuture()
                .whenComplete(
                        (Void ignore, Throwable sourceThreadThrowable) -> {
                            if (sourceThreadThrowable != null) {
                                mailboxProcessor.reportThrowable(sourceThreadThrowable);
                            } else {
                                mailboxProcessor.suspend();
                            }
                        });
    }




    @Override
    protected void cancelTask() {
        if (stopped.compareAndSet(false, true)) {
            cancelOperator();
        }
    }

    private void cancelOperator() {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            if (sourceThread.isAlive()) {
                interruptSourceThread();
            } else if (!sourceThread.getCompletionFuture().isDone()) {
                // sourceThread not alive and completion future not done means source thread
                // didn't start and we need to manually complete the future
                sourceThread.getCompletionFuture().complete(null);
            }
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        super.maybeInterruptOnCancel(toInterrupt, taskName, timeout);
        interruptSourceThread();
    }

    private void interruptSourceThread() {
        // Nothing need to do if the source is finished on restore
        if (operatorChain != null && operatorChain.isTaskDeployedAsFinished()) {
            return;
        }

        if (sourceThread.isAlive()) {
            sourceThread.interrupt();
        }
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions
    ) {
        if (!externallyInducedCheckpoints) {
            if (isSynchronousSavepoint(checkpointOptions.getCheckpointType())) {
                return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
            } else {
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }
        } else if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)) {
            // see FLINK-25256
            throw new IllegalStateException(
                    "Using externally induced sources, we can not enforce taking a full checkpoint."
                            + "If you are restoring from a snapshot in NO_CLAIM mode, please use"
                            + " either CLAIM or LEGACY mode.");
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    private boolean isSynchronousSavepoint(SnapshotType snapshotType) {
        return snapshotType.isSavepoint() && ((SavepointType) snapshotType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        mainMailboxExecutor.execute(
                () ->
                        stopOperatorForStopWithSavepoint(
                                checkpointMetaData.getCheckpointId(),
                                ((SavepointType) checkpointOptions.getCheckpointType())
                                        .shouldDrain()),
                "stop legacy source for stop-with-savepoint --drain");
        return sourceThread
                .getCompletionFuture()
                .thenCompose(
                        ignore ->
                                super.triggerCheckpointAsync(
                                        checkpointMetaData, checkpointOptions));
    }

    private void stopOperatorForStopWithSavepoint(long checkpointId, boolean drain) {
        setSynchronousSavepoint(checkpointId);
        finishingReason =
                drain
                        ? FinishingReason.STOP_WITH_SAVEPOINT_DRAIN
                        : FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN;
        if (mainOperator != null) {
            mainOperator.stop();
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /** Runnable that executes the source function in the head operator. */
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }


        /**
         * 接收数据线程开始工作
         *
         * Flink程序执行的起点
         */
        @Override
        public void run() {
            try {
                if (!operatorChain.isTaskDeployedAsFinished()) {
                    LOG.debug(
                            "Legacy source {} skip execution since the task is finished on restore",
                            getTaskNameWithSubtaskAndId());

//                    这个对象在构造operatorchain时创建出来的
                    // 从sourcestreamtask的主程序开始执行
                    mainOperator.run(lock, operatorChain);
                }
                completeProcessing();
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                if (isCanceled() && ExceptionUtils.findThrowable(t, InterruptedException.class).isPresent()) {
                    completionFuture.completeExceptionally(new CancelTaskException(t));
                } else {
                    completionFuture.completeExceptionally(t);
                }
            }
        }




        private void completeProcessing() throws InterruptedException, ExecutionException {
            if (!isCanceled() && !isFailing()) {
                mainMailboxExecutor
                        .submit(
                                () -> {
                                    // theoretically the StreamSource can implement BoundedOneInput,
                                    // so we need to call it here
                                    final StopMode stopMode = finishingReason.toStopMode();
                                    if (stopMode == StopMode.DRAIN) {
                                        operatorChain.endInput(1);
                                    }
                                    endData(stopMode);
                                },
                                "SourceStreamTask finished processing data.")
                        .get();
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link
         *     #isFailing()} and this thread is not alive (e.g. not started) returns a normally
         *     completed future.
         */
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive()
                    ? CompletableFuture.completedFuture(null)
                    : completionFuture;
        }
    }
}
