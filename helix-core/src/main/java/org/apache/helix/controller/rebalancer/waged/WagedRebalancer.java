package org.apache.helix.controller.rebalancer.waged;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintsRebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Weight-Aware Globally-Even Distribute Rebalancer.
 *
 * @see <a href="https://github.com/apache/helix/wiki/Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer">
 * Design Document
 * </a>
 */
public class WagedRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  // When any of the following change happens, the rebalancer needs to do a global rebalance.
  private static final Set<HelixConstants.ChangeType> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES =
      Collections.unmodifiableSet(new HashSet<>(Arrays
          .asList(HelixConstants.ChangeType.RESOURCE_CONFIG, HelixConstants.ChangeType.CONFIG,
              HelixConstants.ChangeType.INSTANCE_CONFIG)));

  // --------- The following fields are placeholders and need replacement. -----------//
  // TODO Shall we make the metadata store a static threadlocal object as well to avoid reinitialization?
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;
  // ------------------------------------------------------------------------------------//

  // The cluster change detector is a stateful object. Make it static to avoid unnecessary
  // reinitialization.
  private static final ThreadLocal<ResourceChangeDetector> _changeDetector = new ThreadLocal<>();
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;

  private ResourceChangeDetector getChangeDetector() {
    if (_changeDetector.get() == null) {
      _changeDetector.set(new ResourceChangeDetector());
    }
    return _changeDetector.get();
  }

  public WagedRebalancer(HelixManager helixManager) {
    // TODO init the metadata store according to their requirement when integrate, or change to final static method if possible.
    _assignmentMetadataStore = new AssignmentMetadataStore();
    // TODO init the algorithm according to the requirement when integrate.
    _rebalanceAlgorithm = new ConstraintsRebalanceAlgorithm();

    // Use the mapping calculator in DelayedAutoRebalancer for calculating the final assignment
    // output.
    // This calculator will translate the best possible assignment into an applicable state mapping
    // based on the current states.
    // TODO abstract and separate the mapping calculator logic from the DelayedAutoRebalancer
    _mappingCalculator = new DelayedAutoRebalancer();
  }

  /**
   * Compute the new IdealStates for all the resources input. The IdealStates include both the new
   * partition assignment (in the listFiles) and the new replica state mapping (in the mapFields).
   *
   * @param clusterData        The Cluster status data provider.
   * @param resourceMap        A map containing all the rebalancing resources.
   * @param currentStateOutput The present Current State of the cluster.
   * @return A map containing the computed new IdealStates.
   */
  public Map<String, IdealState> computeNewIdealStates(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    LOG.info("Start computing new ideal states for resources: {}", resourceMap.keySet().toString());

    // Find the compatible resources: 1. FULL_AUTO 2. Configured to use the WAGED rebalancer
    resourceMap = resourceMap.entrySet().stream().filter(resourceEntry -> {
      IdealState is = clusterData.getIdealState(resourceEntry.getKey());
      return is != null && is.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO) && is
          .getRebalancerClassName().equals(this.getClass().getName());
    }).collect(Collectors
        .toMap(resourceEntry -> resourceEntry.getKey(), resourceEntry -> resourceEntry.getValue()));

    LOG.info("Valid resources that will be rebalanced by {}: {}", this.getClass().getSimpleName(),
        resourceMap.keySet().toString());

    // Calculate the eventual target assignment based on the current cluster configuration
    Map<String, IdealState> newIdealStates = computeBestPossibleStates(clusterData, resourceMap);

    // Adjust the new best possible states according to the current state.
    // Note that the new ideal state might be an intermediate state between the current state and the best possible state.
    for (IdealState is : newIdealStates.values()) {
      String resourceName = is.getResourceName();
      // Adjust the states according to the current state.
      ResourceAssignment finalAssignment = _mappingCalculator
          .computeBestPossiblePartitionState(clusterData, is, resourceMap.get(resourceName),
              currentStateOutput);

      // Loop all the partition mentioned in the ideal state and update the best possible states with the calculated final states.
      for (String partition : is.getPartitionSet()) {
        Map<String, String> newStateMap = finalAssignment.getReplicaMap(new Partition(partition));
        // if the final states cannot be generated, override the best possible state with empty map.
        is.setInstanceStateMap(partition,
            newStateMap == null ? Collections.emptyMap() : newStateMap);
      }
    }
    return newIdealStates;
  }

  // Coordinate baseline calculation and partial rebalance according to the cluster changes.
  private Map<String, IdealState> computeBestPossibleStates(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap) {
    getChangeDetector().updateSnapshots(clusterData);
    // Get all the modified and new items' information
    Map<HelixConstants.ChangeType, Set<String>> clusterChanges =
        getChangeDetector().getChangeTypes().stream()
            .collect(Collectors.toMap(changeType -> changeType, changeType -> {
              Set<String> itemKeys = new HashSet<>();
              itemKeys.addAll(getChangeDetector().getAdditionsByType(changeType));
              itemKeys.addAll(getChangeDetector().getChangesByType(changeType));
              return itemKeys;
            }));

    if (clusterChanges.keySet().stream()
        .anyMatch(changeType -> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES.contains(changeType))) {
      // For baseline calculation
      // 1. Ignore node status (disable/offline).
      // 2. use the baseline as the previous best possible assignment.
      // TODO make the Baseline calculation async if complicated algorithm is used for the Baseline
      LOG.info("Calculate for the new baseline.");
      Map<String, ResourceAssignment> baseline =
          calculateAssignment(clusterData, clusterChanges, resourceMap,
              clusterData.getAllInstances(), Collections.emptyMap(),
              _assignmentMetadataStore.getBaseline());
      _assignmentMetadataStore.persistBaseline(baseline);
      // Inject a cluster config change for recalculating all the replicas on a baseline change.
      clusterChanges.putIfAbsent(HelixConstants.ChangeType.CONFIG, Collections.emptySet());
    }

    // Partial rebalance
    Set<String> activeInstances = clusterData.getEnabledLiveInstances();
    LOG.info("Calculate for the new best possible assignment.");
    Map<String, ResourceAssignment> newAssignment =
        calculateAssignment(clusterData, clusterChanges, resourceMap, activeInstances,
            _assignmentMetadataStore.getBaseline(),
            _assignmentMetadataStore.getBestPossibleAssignment());
    // TODO We need to test to confirm if persisting the final assignment is better than this one.
    _assignmentMetadataStore.persistBestPossibleAssignment(newAssignment);

    // Convert the assignments into IdealState for additional calculation
    Map<String, IdealState> finalIdealState = new HashMap<>();
    Map<String, Map<String, Integer>> resourceStatePriorityMap =
        new HashMap<>(); // to avoid duplicate calculation
    for (String resourceName : newAssignment.keySet()) {
      IdealState currentIdealState = clusterData.getIdealState(resourceName);
      if (currentIdealState != null) {
        String stateModelDef = currentIdealState.getStateModelDefRef();
        resourceStatePriorityMap
            .put(resourceName, clusterData.getStateModelDef(stateModelDef).getStatePriorityMap());
        // Create a new IdealState contains the new calculated mapping.
        IdealState newIdeaState =
            generateIdealStateWithPreferenceList(resourceName, currentIdealState,
                newAssignment.get(resourceName), resourceStatePriorityMap.get(resourceName));
        finalIdealState.put(resourceName, newIdeaState);
      } else {
        LOG.error(
            "Cannot find the current IdealState of resource {} for generating the best possible assignment. Will ignore the resource assignment.",
            resourceName);
      }
    }

    return finalIdealState;
  }

  /**
   * Generate the cluster model based on the input and calculate the optimal assignment.
   *
   * @param clusterData                the cluster data cache.
   * @param clusterChanges             the detected cluster changes.
   * @param resourceMap                the rebalancing resources.
   * @param activeNodes                the alive and enabled nodes.
   * @param baseline                   the baseline assignment for the algorithm as a reference.
   * @param prevBestPossibleAssignment the previous best possible assignment for the algorithm as a reference.
   * @return the new optimal assignment for the resources.
   */
  private Map<String, ResourceAssignment> calculateAssignment(
      ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      Set<String> activeNodes, Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> prevBestPossibleAssignment) {
    long startTime = System.currentTimeMillis();
    LOG.info("Start calculating for an assignment");
    ClusterModel clusterModel = ClusterModelProvider
        .generateClusterModel(clusterData, resourceMap, activeNodes, clusterChanges, baseline,
            prevBestPossibleAssignment);
    Map<String, ResourceAssignment> newAssignment =
        _rebalanceAlgorithm.rebalance(clusterModel, null);
    LOG.info("Finish calculating. Time spent: {}ms.", System.currentTimeMillis() - startTime);
    return newAssignment;
  }

  // Generate a new IdealState based on the input newAssignment.
  private IdealState generateIdealStateWithPreferenceList(String resourceName,
      IdealState currentIdealState, ResourceAssignment newAssignment,
      Map<String, Integer> statePriorityMap) {
    IdealState newIdealState = new IdealState(resourceName);
    // Copy the simple fields
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    // Sort the preference list according to state priority.
    newIdealState.setPreferenceLists(getPreferenceList(newAssignment, statePriorityMap));
    // Note the state mapping in the new assignment won't be directly propagate to the map fields.
    // The rebalancer will calculate for the final state mapping considering the current states.
    return newIdealState;
  }

  // Extract the preference list from the state mapping based on state priority.
  private Map<String, List<String>> getPreferenceList(ResourceAssignment newAssignment,
      Map<String, Integer> statePriorityMap) {
    Map<String, List<String>> preferenceList = new HashMap<>();
    for (Partition partition : newAssignment.getMappedPartitions()) {
      List<String> nodes = new ArrayList<>(newAssignment.getReplicaMap(partition).keySet());
      // To ensure backward compatibility, sort the preference list according to state priority.
      nodes.sort((node1, node2) -> {
        int statePriority1 =
            statePriorityMap.get(newAssignment.getReplicaMap(partition).get(node1));
        int statePriority2 =
            statePriorityMap.get(newAssignment.getReplicaMap(partition).get(node2));
        if (statePriority1 == statePriority2) {
          return node1.compareTo(node2);
        } else {
          return statePriority1 - statePriority2;
        }
      });
      preferenceList.put(partition.getPartitionName(), nodes);
    }
    return preferenceList;
  }
}
