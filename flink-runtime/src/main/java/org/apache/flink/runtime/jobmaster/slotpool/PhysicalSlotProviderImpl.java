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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The provider serves physical slot requests. */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

    private final SlotSelectionStrategy slotSelectionStrategy;

    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }

    /**
     * 申请物理slot
     *
     * @param physicalSlotRequest slot requirements
     * @return
     */
    @Override
    public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest) {
        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
        ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

        LOG.debug(
                "Received slot request [{}] with resource requirements: {}",
                slotRequestId,
                resourceProfile);




        /**
         * 1 先尝试从slotpool中进行申请
         */
        Optional<PhysicalSlot> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile);



        /**
         * 2 如果没有可用，再尝试申请新的slot
         */
        CompletableFuture<PhysicalSlot> slotFuture;
        slotFuture =
                availablePhysicalSlot
                        .map(CompletableFuture::completedFuture)
                        .orElseGet(
                                () ->
                                        requestNewSlot( // 申请新的slot
                                                slotRequestId,
                                                resourceProfile,
                                                slotProfile.getPreferredAllocations(),
                                                physicalSlotRequest.willSlotBeOccupiedIndefinitely()
                                        )
                        );


        return slotFuture.thenApply(
                physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot)
        );
    }

    /**
     * 尝试从slotpool获取slot
     *
     * @param slotRequestId
     * @param slotProfile
     * @return
     */
    private Optional<PhysicalSlot> tryAllocateFromAvailable(SlotRequestId slotRequestId, SlotProfile slotProfile) {

//        可以申请的slot列表
        Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
                slotPool.getAvailableSlotsInformation().stream()
                        .map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot)
                        .collect(Collectors.toList());

//        定义获得最好的slot策略
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
                slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

        return selectedAvailableSlot.flatMap(
                slotInfoAndLocality ->
//                        分配slot
                        slotPool.allocateAvailableSlot(
                                slotRequestId,
                                slotInfoAndLocality.getSlotInfo().getAllocationId(),
                                slotProfile.getPhysicalSlotResourceProfile()));
    }



    /**
     * 申请新的slot资源
     *
     * @param slotRequestId
     * @param resourceProfile
     * @param preferredAllocations
     * @param willSlotBeOccupiedIndefinitely
     * @return
     */
    private CompletableFuture<PhysicalSlot> requestNewSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean willSlotBeOccupiedIndefinitely) {

        if (willSlotBeOccupiedIndefinitely) {
            return slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, preferredAllocations, null);
        } else {
            return slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile, preferredAllocations);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
