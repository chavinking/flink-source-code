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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;

import javax.annotation.Nullable;

import java.util.List;

/**
 * It enables the execution of a graph of {@link Transformation}s generated by the {@link Planner}.
 *
 * <p>This uncouples the {@link TableEnvironment} from any given runtime.
 *
 * <p>Note that not every table program calls {@link #createPipeline(List, ReadableConfig, String)}
 * or {@link #execute(Pipeline)}. When bridging to DataStream API, this interface serves as a
 * communication layer to the final pipeline executor via {@code StreamExecutionEnvironment}.
 *
 * @see ExecutorFactory
 */
@Internal
public interface Executor {

    /** Gives read-only access to the configuration of the executor. */
    ReadableConfig getConfiguration();

    /**
     * Translates the given transformations to a {@link Pipeline}.
     *
     * @param transformations list of transformations
     * @param tableConfiguration table-specific configuration options
     * @param defaultJobName default job name if not specified via {@link PipelineOptions#NAME}
     * @return The pipeline representing the transformations.
     */
    Pipeline createPipeline(
            List<Transformation<?>> transformations,
            ReadableConfig tableConfiguration,
            @Nullable String defaultJobName
    );

    /**
     * Executes the given pipeline.
     *
     * @param pipeline the pipeline to execute
     * @return The result of the job execution, containing elapsed time and accumulators.
     * @throws Exception which occurs during job execution.
     */
    JobExecutionResult execute(Pipeline pipeline) throws Exception;

    /**
     * Executes the given pipeline asynchronously.
     *
     * @param pipeline the pipeline to execute
     * @return A {@link JobClient} that can be used to communicate with the submitted job, completed
     *     on submission succeeded.
     * @throws Exception which occurs during job execution.
     */
    JobClient executeAsync(Pipeline pipeline) throws Exception;

    /**
     * Checks whether checkpointing is enabled.
     *
     * @return True if checkpointing is enables, false otherwise.
     */
    boolean isCheckpointingEnabled();
}
