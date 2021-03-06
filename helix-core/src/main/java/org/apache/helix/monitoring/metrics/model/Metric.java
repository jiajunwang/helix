package org.apache.helix.monitoring.metrics.model;

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;

/**
 * Defines a generic metric interface.
 * @param <T> type of input value for the metric
 */
public interface Metric<T> {

  /**
   * Gets the name of the metric.
   */
  String getMetricName();

  /**
   * Prints the metric along with its name.
   */
  String toString();

  /**
   * Returns the most recently emitted value for the metric at the time of the call.
   * @return metric value
   */
  T getLastEmittedMetricValue();

  /**
   * Returns the underlying DynamicMetric.
   */
  DynamicMetric getDynamicMetric();
}
