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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.executiongraph.failover.flip1.LogicalPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalTopology}. It is an adapter of {@link JobGraph}. */
public class DefaultLogicalTopology implements LogicalTopology {

    private final List<DefaultLogicalVertex> verticesSorted;

    private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;

    private final Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap;

    private DefaultLogicalTopology(final List<JobVertex> jobVertices) {
        checkNotNull(jobVertices);

        this.verticesSorted = new ArrayList<>(jobVertices.size());
        this.idToVertexMap = new HashMap<>();
        this.idToResultMap = new HashMap<>();

        // 构建vertex和result
        buildVerticesAndResults(jobVertices);
    }

    public static DefaultLogicalTopology fromJobGraph(final JobGraph jobGraph) {
        checkNotNull(jobGraph);

        return fromTopologicallySortedJobVertices(
                jobGraph.getVerticesSortedTopologicallyFromSources());
    }

    public static DefaultLogicalTopology fromTopologicallySortedJobVertices(final List<JobVertex> jobVertices) {
        return new DefaultLogicalTopology(jobVertices);
    }

    private void buildVerticesAndResults(final Iterable<JobVertex> topologicallySortedJobVertices) {
        final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
        final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever = this::getResult;

//        将JobVertex封装成DefaultLogicalVertex，将IntermediateDataSet封装成DefaultLogicalResult
        for (JobVertex jobVertex : topologicallySortedJobVertices) {
//            初始化DefaultLogicalVertex，设置jobvertex和其结果集
            final DefaultLogicalVertex logicalVertex = new DefaultLogicalVertex(jobVertex, resultRetriever);
            this.verticesSorted.add(logicalVertex);
            this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);

//            jobVertex.getProducedDataSets()：代表jobVertex的输出结果集合
            for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
                final DefaultLogicalResult logicalResult = new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
                idToResultMap.put(logicalResult.getId(), logicalResult);
            }
        }

    }

    @Override
    public Iterable<DefaultLogicalVertex> getVertices() {
        return verticesSorted;
    }

    public DefaultLogicalVertex getVertex(final JobVertexID vertexId) {
        return Optional.ofNullable(idToVertexMap.get(vertexId))
                .orElseThrow(() -> new IllegalArgumentException("can not find vertex: " + vertexId));
    }

    private DefaultLogicalResult getResult(final IntermediateDataSetID resultId) {
        return Optional.ofNullable(idToResultMap.get(resultId))
                .orElseThrow(() -> new IllegalArgumentException("can not find result: " + resultId));
    }

    @Override
    public Iterable<DefaultLogicalPipelinedRegion> getAllPipelinedRegions() {

        final Set<Set<LogicalVertex>> regionsRaw = LogicalPipelinedRegionComputeUtil.computePipelinedRegions(verticesSorted);

        final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
        for (Set<LogicalVertex> regionVertices : regionsRaw) {
            regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
        }
        return regions;
    }

}
