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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    // 调度拓扑
    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted = new IdentityHashMap<>();

    /** All produced partition groups of one schedulingPipelinedRegion. */
    private final Map<SchedulingPipelinedRegion, Set<ConsumedPartitionGroup>>
            producedPartitionGroupsOfRegion = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private final Set<SchedulingPipelinedRegion> scheduledRegions =
            Collections.newSetFromMap(new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        initProducedPartitionGroupsOfRegion();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region = schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted.computeIfAbsent(region, r -> new ArrayList<>()).add(vertex.getId());
        }
    }

    private void initProducedPartitionGroupsOfRegion() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Set<ConsumedPartitionGroup> producedPartitionGroupsSetOfRegion = new HashSet<>();
            for (SchedulingExecutionVertex executionVertex : region.getVertices()) {
                producedPartitionGroupsSetOfRegion.addAll(
                        IterableUtils.toStream(executionVertex.getProducedResults())
                                .flatMap(
                                        partition ->
                                                partition.getConsumedPartitionGroups().stream())
                                .collect(Collectors.toSet()));
            }
            producedPartitionGroupsOfRegion.put(region, producedPartitionGroupsSetOfRegion);
        }
    }

    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>> producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup : pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup : pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions = producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getBlockingDownstreamRegionsOfVertex(
            SchedulingExecutionVertex executionVertex) {
        return IterableUtils.toStream(executionVertex.getProducedResults())
                .filter(partition -> !partition.getResultType().canBePipelinedConsumed())
                .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                .filter(
                        group ->
                                crossRegionConsumedPartitionGroups.contains(group)
                                        || group.areAllPartitionsFinished())
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }


    /**
     * 这里在部署过程中包含了一次类型封装
     *
     */
    @Override
    public void startScheduling() {

//        获取 source 对应的 Region 数据 ，即可以理解为从源端开始
        /**
         * 这段代码的作用是从 schedulingTopology 中获取所有的 Pipeline Region，然后从中筛选出所有的 Source Region，最后将它们作为 Set 返回。
         *
         * 具体来说，代码的执行过程如下：
         *
         * schedulingTopology.getAllPipelinedRegions() 方法返回一个 Iterable 对象，其中包含了所有的 Pipeline Region。
         *
         * IterableUtils.toStream() 方法将 Iterable 对象转换成一个 Stream 对象，这样我们就可以使用 Stream 的各种操作对 Pipeline Region 进行处理。
         *
         * filter() 方法用于对 Stream 中的元素进行筛选，只保留符合条件的元素。这里使用了一个自定义的 isSourceRegion 方法作为筛选条件，该方法接受一个 Pipeline Region 作为参数，返回值为布尔型。当 Pipeline Region 是一个 Source Region 时，该方法返回 true，否则返回 false。
         *
         * collect() 方法将 Stream 中符合条件的元素收集到一个 Set 中，并将 Set 作为方法的返回值。
         *
         * 因此，整个代码的作用就是获取所有的 Source Region，并将它们作为一个 Set 返回。
         *
         * 结果【sourceRegions=pipelinedRegions】
         */
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions()) // 拿到的是 pipelinedRegions 对象 ，样例代码只有一个值，内部包含5个单元
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());

//        从source region开始调度
        maybeScheduleRegions(sourceRegions);
    }

    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup) || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(
                    getBlockingDownstreamRegionsOfVertex(
                            schedulingTopology.getVertex(executionVertexId)));
        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}



    /**
     * 这里进行了一次封装
     *
     * 这段代码的含义是在一个循环中，对当前待调度的 Region 及其后继 Region 进行调度，直到没有更多的 Region 需要被调度为止。
     *
     * 具体来说，代码的执行过程如下：
     *
     * 首先，代码先定义了一个名为 nextRegions 的变量，它是一个 Set 类型，用于存储当前待调度的 Region 的后继 Region。初始化时，nextRegions 被赋值为一个空的 Set。
     *
     * 进入 while 循环。在每次循环中，代码首先检查 nextRegions 是否为空。如果为空，说明当前待调度的 Region 及其后继 Region 已经全部被调度完毕，循环结束。
     *
     * 如果 nextRegions 不为空，则调用 addSchedulableAndGetNextRegions() 方法对其中的所有 Region 进行调度，并返回调度后的下一批待调度的 Region。
     * 这些待调度的 Region 将被添加到 regionsToSchedule 中，用于下一次循环。
     * 如果 nextRegions 中的所有 Region 都已经被调度过了，那么在调用 addSchedulableAndGetNextRegions() 方法时，返回的下一批待调度的 Region 将是一个空的 Set。
     *
     * 在下一次循环中，如果 nextRegions 仍然为空，那么循环将结束。否则，循环将继续执行，直到所有的 Region 都被调度完毕。
     *
     * 因此，这段代码的作用是在一个循环中，不断地对当前待调度的 Region 及其后继 Region 进行调度，直到所有的 Region 都被调度完毕为止。
     *
     * @param regions ：这个值是对代码的执行单元的封装，测试样例只有一个值，即封装到了一个值里【PipelinedRegions】
     */
    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        while (!nextRegions.isEmpty()) {
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }

        // schedule regions in topological order.
        SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(schedulingTopology, regionsToSchedule).forEach(this::scheduleRegion);
    }




    private Set<SchedulingPipelinedRegion> addSchedulableAndGetNextRegions(
            Set<SchedulingPipelinedRegion> currentRegions,
            Set<SchedulingPipelinedRegion> regionsToSchedule) {

        Set<SchedulingPipelinedRegion> nextRegions = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        final Set<ConsumedPartitionGroup> visitedConsumedPartitionGroups = new HashSet<>();

        for (SchedulingPipelinedRegion currentRegion : currentRegions) {
            if (isRegionSchedulable(currentRegion, consumableStatusCache, regionsToSchedule)) {
                regionsToSchedule.add(currentRegion);
                producedPartitionGroupsOfRegion
                        .getOrDefault(currentRegion, Collections.emptySet())
                        .forEach(
                                (producedPartitionGroup) -> {
                                    if (!producedPartitionGroup
                                            .getResultPartitionType()
                                            .canBePipelinedConsumed()) {
                                        return;
                                    }
                                    // If this group has been visited, there is no need
                                    // to repeat the determination.
                                    if (visitedConsumedPartitionGroups.contains(
                                            producedPartitionGroup)) {
                                        return;
                                    }
                                    visitedConsumedPartitionGroups.add(producedPartitionGroup);
                                    nextRegions.addAll(
                                            partitionGroupConsumerRegions.getOrDefault(
                                                    producedPartitionGroup,
                                                    Collections.emptySet()));
                                });
            }
        }
        return nextRegions;
    }



    private boolean isRegionSchedulable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        return !regionToSchedule.contains(region)
                && !scheduledRegions.contains(region)
                && areRegionInputsAllConsumable(region, consumableStatusCache, regionToSchedule);
    }


    /**
     * 在 Flink 中，Region 是指一个连通的 ExecutionVertex 集合，其中所有的 ExecutionVertex 之间通过网络边连接。在调度过程中，Flink 会将 ExecutionVertex 分配到不同的 TaskManager 中执行。为了保证任务之间的数据传输效率，Flink 会将一些相邻的 ExecutionVertex 分配到同一个 TaskManager 中，并将它们打包成一个 Region。
     *
     * 在 Flink 中，Region 有两种类型：
     *
     * Pipelined Region：它是一个顺序计算的 ExecutionVertex 集合，其中每个 ExecutionVertex 会消费其前置 ExecutionVertex 的输出，并生成新的数据流传递给后置 ExecutionVertex。
     *
     * Bounded Region：它是一个非顺序计算的 ExecutionVertex 集合，其中每个 ExecutionVertex 可以独立计算，不需要其他 ExecutionVertex 的输出。
     *
     * 在代码中，topology.getPipelinedRegionOfVertex(vertexId) 方法可以获取指定 ExecutionVertex 所属的 Pipelined Region。因此，这段代码的作用是从调度图中找出所有属于一组指定 Pipelined Region 的 ExecutionVertex，并将这些 ExecutionVertex 所属的 Region 进行去重，最终返回一个列表。
     *
     * @param region
     */
    private void scheduleRegion(final SchedulingPipelinedRegion region) {
        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");
        scheduledRegions.add(region);
        /**
         * region：PipelinedRegions
         * regionVerticesSorted.get(region):获取到最小执行单元的集合，示例中一共有5个执行单元，返回的是一个list集合
         */
        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(consumedPartitionGroup, region, regionToSchedule)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        (group) ->
                                isDownstreamConsumedPartitionGroupSchedulable(group, regionToSchedule))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                if (!scheduledRegions.contains(producerRegion)
                        && !regionToSchedule.contains(producerRegion)) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.CONSUMABLE) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final SchedulingPipelinedRegion pipelinedRegion,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)) {
                    SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                    if (!regionToSchedule.contains(producerRegion)
                            && !scheduledRegions.contains(producerRegion)) {
                        return false;
                    }
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.CONSUMABLE) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
