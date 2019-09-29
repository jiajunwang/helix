package org.apache.helix.controller.rebalancer.waged.model;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a possible allocation of the replication.
 * Note that any usage updates to the AssignableNode are not thread safe.
 */
public class AssignableNode implements Comparable<AssignableNode> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableNode.class.getName());

  // Immutable node properties
  private final String _instanceName;
  private final String _topologyName;
  private final String _faultZone;
  // Maximum number of the partitions that can be assigned to the instance.
  private final int _maxPartition;
  private final ImmutableSet<String> _instanceTags;
  private final ImmutableMap<String, List<String>> _disabledPartitionsMap;
  private final ImmutableMap<String, Integer> _maxAllowedCapacity;

  // Mutable (Dynamic) Instance Properties
  // A map of <resource name, <partition name, replica>> that tracks the replicas assigned to the
  // node.
  private Map<String, Map<String, AssignableReplica>> _currentAssignedReplicaMap;
  // A map of <capacity key, capacity value> that tracks the current available node capacity
  private Map<String, Integer> _remainingCapacity;
  // The maximum capacity utilization (0.0 - 1.0) across all the capacity categories.
  private float _highestCapacityUtilization;

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculates
   * currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the
   * clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and
   * ResourceConfig could
   * subject to change. If the assumption is no longer true, this function should become private.
   */
  AssignableNode(ClusterConfig clusterConfig, InstanceConfig instanceConfig) {
    _instanceName = instanceConfig.getInstanceName();
    if (clusterConfig.isTopologyAwareEnabled()) {
      _faultZone = computeFaultZone(clusterConfig.getTopology(), clusterConfig.getFaultZoneType(),
          instanceConfig);
      _topologyName = computeDomainName(clusterConfig.getTopology(), instanceConfig);
    } else {
      // Instance name is the default fault zone if topology awareness is false.
      _faultZone = _instanceName;
      _topologyName = _instanceName;
    }
    _instanceTags = ImmutableSet.copyOf(instanceConfig.getTags());
    _disabledPartitionsMap = ImmutableMap.copyOf(instanceConfig.getDisabledPartitionsMap());
    // make a copy of max capacity
    Map<String, Integer> instanceCapacity = fetchInstanceCapacity(clusterConfig, instanceConfig);
    _maxAllowedCapacity = ImmutableMap.copyOf(instanceCapacity);
    _remainingCapacity = new HashMap<>(instanceCapacity);
    _maxPartition = clusterConfig.getMaxPartitionsPerInstance();
    _currentAssignedReplicaMap = new HashMap<>();
    _highestCapacityUtilization = 0f;
  }

  /**
   * This function should only be used to assign a set of new partitions that are not allocated on
   * this node. It's because the any exception could occur at the middle of batch assignment and the
   * previous finished assignment cannot be reverted
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  void assignInitBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      // TODO: the exception could occur in the middle of for loop and the previous added records cannot be reverted
      addToAssignmentRecord(replica);
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                : totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    for (String capacityKey : totalPartitionCapacity.keySet()) {
      updateCapacityAndUtilization(capacityKey, totalPartitionCapacity.get(capacityKey));
    }
  }

  /**
   * Assign a replica to the node.
   * @param assignableReplica - the replica to be assigned
   */
  void assign(AssignableReplica assignableReplica) {
    addToAssignmentRecord(assignableReplica);
    assignableReplica.getCapacity().entrySet().stream()
            .forEach(capacity -> updateCapacityAndUtilization(capacity.getKey(), capacity.getValue()));
  }

  /**
   * Release a replica from the node.
   * If the replication is not on this node, the assignable node is not updated.
   * @param replica - the replica to be released
   */
  void release(AssignableReplica replica) throws IllegalArgumentException {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();

    // Check if the release is necessary
    if (!_currentAssignedReplicaMap.containsKey(resourceName)) {
      LOG.warn("Resource {} is not on node {}. Ignore the release call.", resourceName,
          getName());
      return;
    }

    Map<String, AssignableReplica> partitionMap = _currentAssignedReplicaMap.get(resourceName);
    if (!partitionMap.containsKey(partitionName)
        || !partitionMap.get(partitionName).equals(replica)) {
      LOG.warn("Replica {} is not assigned to node {}. Ignore the release call.",
          replica.toString(), getName());
      return;
    }

    AssignableReplica removedReplica = partitionMap.remove(partitionName);
    // Recalculate utilization because of release
    _highestCapacityUtilization = 0;
    removedReplica.getCapacity().entrySet().stream()
        .forEach(entry -> updateCapacityAndUtilization(entry.getKey(), -1 * entry.getValue()));
  }

  /**
   * @return A set of all assigned replicas on the node.
   */
  public Set<AssignableReplica> getAssignedReplicas() {
    return _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return The current assignment in a map of <resource name, set of partition names>
   */
  public Map<String, Set<String>> getAssignedPartitionsMap() {
    Map<String, Set<String>> assignmentMap = new HashMap<>();
    for (String resourceName : _currentAssignedReplicaMap.keySet()) {
      assignmentMap.put(resourceName, _currentAssignedReplicaMap.get(resourceName).keySet());
    }
    return assignmentMap;
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names in the specified resource.
   */
  public Set<String> getAssignedPartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).keySet();
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names with the top state in the
   *         specified resource.
   */
  public Set<String> getAssignedTopStatePartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).entrySet()
        .stream().filter(partitionEntry -> partitionEntry.getValue().isReplicaTopState())
        .map(partitionEntry -> partitionEntry.getKey()).collect(Collectors.toSet());
  }

  /**
   * @return The total count of assigned top state partitions.
   */
  public int getAssignedTopStatePartitionsCount() {
    return (int) _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream())
        .filter(AssignableReplica::isReplicaTopState).count();
  }

  /**
   * @return The total count of assigned replicas.
   */
  public int getAssignedReplicaCount() {
    return _currentAssignedReplicaMap.values().stream().mapToInt(Map::size).sum();
  }

  /**
   * @return The current available capacity.
   */
  public Map<String, Integer> getRemainingCapacity() {
    return _remainingCapacity;
  }

  /**
   * @return A map of <capacity category, capacity number> that describes the max capacity of the
   *         node.
   */
  public Map<String, Integer> getMaxCapacity() {
    return _maxAllowedCapacity;
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically returns the highest utilization number among all the capacity
   * categories.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getHighestCapacityUtilization() {
    return _highestCapacityUtilization;
  }

  public String getName() {
    return _topologyName;
  }

  String getInstanceName() {
    return _instanceName;
  }

  public Set<String> getInstanceTags() {
    return _instanceTags;
  }

  public String getFaultZone() {
    return _faultZone;
  }

  public boolean hasFaultZone() {
    return _faultZone != null;
  }

  /**
   * @return A map of <resource name, set of partition names> contains all the partitions that are
   *         disabled on the node.
   */
  public Map<String, List<String>> getDisabledPartitionsMap() {
    return _disabledPartitionsMap;
  }

  /**
   * @return The max partition count that are allowed to be allocated on the node.
   */
  public int getMaxPartition() {
    return _maxPartition;
  }

  /**
   * Compute the fault zone id based on the domain and fault zone type when topology is enabled.
   * For example, when
   * the domain is "zone=2, instance=testInstance" and the fault zone type is "zone", this function
   * returns "2".
   * If cannot find the fault zone id, this function leaves the fault zone id as the instance name.
   * TODO merge this logic with Topology.java tree building logic.
   * For now, the WAGED rebalancer has a more strict topology def requirement.
   * Any missing field will cause an invalid topology config exception.
   */
  private String computeFaultZone(String topologyStr, String faultZoneType,
      InstanceConfig instanceConfig) {
    if (topologyStr == null || faultZoneType == null) {
      LOG.debug("Topology configuration is not complete. Topology define: {}, Fault Zone Type: {}",
          topologyStr, faultZoneType);
      // Use the instance name, or the deprecated ZoneId field (if exists) as the default fault
      // zone.
      String zoneId = instanceConfig.getZoneId();
      return zoneId == null ? instanceConfig.getInstanceName() : zoneId;
    } else {
      return getDomainNameStr(topologyStr, faultZoneType, instanceConfig.getDomainAsMap());
    }
  }

  /**
   * Compute the full domain name of the instance based on the topology.
   */
  private String computeDomainName(String topologyStr, InstanceConfig instanceConfig) {
    if (topologyStr == null) {
      LOG.debug("Topology configuration is empty. Use the instance name as the domain name.");
      return instanceConfig.getInstanceName();
    } else {
      return getDomainNameStr(topologyStr, null, instanceConfig.getDomainAsMap());
    }
  }

  /**
   * Get the shortest domain name of the instance that contains the target domain key based on the
   * topology definition.
   * For example, if topology def is "/DOMAIN/ZONE/HOST", and the target key is "ZONE", the method
   * will return the instance domain name that consists of DOMAIN and ZONE only.
   * @param topologyDefStr      the topology definition in a string.
   * @param targetDomainKey     the target domain key.
   *                            If this param is null, return the full domain name.
   * @param instanceDomainAsMap the instance domain information in a map.
   */
  private String getDomainNameStr(String topologyDefStr, String targetDomainKey,
      Map<String, String> instanceDomainAsMap) {
    String[] topologyDef = topologyDefStr.trim().split("/");
    if (topologyDef.length == 0) {
      throw new HelixException("The configured topology definition is empty");
    }
    if (targetDomainKey != null && Arrays.stream(topologyDef)
        .noneMatch(type -> type.equals(targetDomainKey))) {
      throw new HelixException(String
          .format("The configured topology definition does not contain the required Domain Key {}.",
              targetDomainKey));
    }
    if (instanceDomainAsMap == null) {
      throw new HelixException(
          String.format("The domain configuration of node %s is not configured", _instanceName));
    } else {
      StringBuilder domainNameBuilder = new StringBuilder();
      for (String key : topologyDef) {
        if (!key.isEmpty()) {
          if (instanceDomainAsMap.containsKey(key)) {
            domainNameBuilder.append(instanceDomainAsMap.get(key));
            domainNameBuilder.append('/');
          } else {
            throw new HelixException(String
                .format("The domain configuration of node %s is not complete. Key %s is not found.",
                    _instanceName, key));
          }
          if (targetDomainKey != null && key.equals(targetDomainKey)) {
            break;
          }
        }
      }
      return domainNameBuilder.toString();
    }
  }

  /**
   * @throws HelixException if the replica has already been assigned to the node.
   */
  private void addToAssignmentRecord(AssignableReplica replica) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (_currentAssignedReplicaMap.containsKey(resourceName)
        && _currentAssignedReplicaMap.get(resourceName).containsKey(partitionName)) {
      throw new HelixException(String.format(
          "Resource %s already has a replica with state %s from partition %s on node %s",
          replica.getResourceName(), replica.getReplicaState(), replica.getPartitionName(),
          getName()));
    } else {
      _currentAssignedReplicaMap.computeIfAbsent(resourceName, key -> new HashMap<>())
          .put(partitionName, replica);
    }
  }

  private void updateCapacityAndUtilization(String capacityKey, int usage) {
    if (!_remainingCapacity.containsKey(capacityKey)) {
      //if the capacityKey belongs to replicas does not exist in the instance's capacity,
      // it will be treated as if it has unlimited capacity of that capacityKey
      return;
    }
    int newCapacity = _remainingCapacity.get(capacityKey) - usage;
    _remainingCapacity.put(capacityKey, newCapacity);
    // For the purpose of constraint calculation, the max utilization cannot be larger than 100%.
    float utilization = Math.min((float) (_maxAllowedCapacity.get(capacityKey) - newCapacity)
        / _maxAllowedCapacity.get(capacityKey), 1);
    _highestCapacityUtilization = Math.max(_highestCapacityUtilization, utilization);
  }

  /**
   * Get and validate the instance capacity from instance config.
   *
   * @throws HelixException if any required capacity key is not configured in the instance config.
   */
  private Map<String, Integer> fetchInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    Map<String, Integer> instanceCapacity = instanceConfig.getInstanceCapacityMap();
    if (instanceCapacity.isEmpty()) {
      instanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
    }
    // Remove all the non-required capacity items from the map.
    instanceCapacity.keySet().retainAll(requiredCapacityKeys);
    // All the required keys must exist in the instance config.
    if (!instanceCapacity.keySet().containsAll(requiredCapacityKeys)) {
      throw new HelixException(String.format(
          "The required capacity keys %s are not fully configured in the instance %s capacity map %s.",
          requiredCapacityKeys.toString(), instanceConfig.getInstanceName(),
          instanceCapacity.toString()));
    }
    return instanceCapacity;
  }

  @Override
  public int hashCode() {
    return _topologyName.hashCode();
  }

  @Override
  public int compareTo(AssignableNode o) {
    return _topologyName.compareTo(o._topologyName);
  }

  @Override
  public String toString() {
    return _topologyName;
  }
}
