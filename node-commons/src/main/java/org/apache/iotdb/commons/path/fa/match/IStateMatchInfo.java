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

package org.apache.iotdb.commons.path.fa.match;

import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;

import java.util.Iterator;

/**
 * This interface defines the behaviour of a state match info, which helps make decision during FA
 * Graph traversing
 */
public interface IStateMatchInfo {

  boolean hasFinalState();

  boolean hasOnlyPreciseMatchTransition();

  boolean hasNoPreciseMatchTransition();

  boolean isSingleBatchMatchTransition();

  IFAState getOneMatchedState();

  void addMatchedState(IFAState state);

  /**
   * @param stateOrdinal the target state's ordinal of matched order
   * @return the ordinal(th) matched state
   */
  IFAState getMatchedState(int stateOrdinal);

  int getMatchedStateSize();

  /** @return the ordinal of the source state in matched order */
  int getSourceStateOrdinal();

  /** @param sourceStateOrdinal the ordinal of the source state in matched order */
  void setSourceStateOrdinal(int sourceStateOrdinal);

  Iterator<IFATransition> getSourceTransitionIterator();

  void setSourceTransitionIterator(Iterator<IFATransition> sourceTransitionIterator);
}
