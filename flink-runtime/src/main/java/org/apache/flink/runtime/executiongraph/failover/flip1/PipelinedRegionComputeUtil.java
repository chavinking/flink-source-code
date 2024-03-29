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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Vertex;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Common utils for computing pipelined regions. */
public final class PipelinedRegionComputeUtil {

    /**
     * 这里封装着一个用来合并的算法策略
     *
     * @param topologicallySortedVertices
     * @param getMustBePipelinedConsumedResults
     * @return
     * @param <V>
     * @param <R>
     */
    static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>>
            Map<V, Set<V>> buildRawRegions(
                    final Iterable<? extends V> topologicallySortedVertices,            // jobvertex对应的逻辑集合
                    final Function<V, Iterable<R>> getMustBePipelinedConsumedResults) { // 传入的是一个函数

        final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (V vertex : topologicallySortedVertices) {

            Set<V> currentRegion = new HashSet<>();
            currentRegion.add(vertex);
            vertexToRegion.put(vertex, currentRegion);

            // Each vertex connected through not mustBePipelined consumingConstraint is considered as a single region.
            // consumedResult ：满足条件的上游输入数据集合
            for (R consumedResult : getMustBePipelinedConsumedResults.apply(vertex)) {
                final V producerVertex = consumedResult.getProducer(); // 上游数据集的输入是上一个jobvertex对应的vertex
                final Set<V> producerRegion = vertexToRegion.get(producerVertex);

                if (producerRegion == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getId()
                                    + " failover region is null"
                                    + " while calculating failover region for the consumer task "
                                    + vertex.getId()
                                    + ". This should be a failover region building bug.");
                }

                // check if it is the same as the producer region, if so skip the merge this check can significantly reduce compute complexity in All-to-All PIPELINED edge case
                // 检查它是否与生产者区域相同，如果是，则跳过合并，此检查可以显着降低在All-to-All pipeline边缘情况下的计算复杂性
                if (currentRegion != producerRegion) {
                    currentRegion = VertexGroupComputeUtil.mergeVertexGroups(currentRegion, producerRegion, vertexToRegion);
                }
            }
        }

        return vertexToRegion;
    }

    private PipelinedRegionComputeUtil() {}
}
