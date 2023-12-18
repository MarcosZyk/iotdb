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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotPinnedException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;

/**
 * This class implemented the cache management, involving the cache status management on per MNode
 * and cache eviction. All the nodes in memory are still basically organized as a trie via the
 * container and parent reference in each node. Some extra data structure is used to help manage the
 * node cached in memory.
 *
 * <p>The cache eviction on node is actually evicting a subtree from the MTree.
 *
 * <p>All the cached nodes are divided into two parts by their cache status, evictable nodes and
 * none evictable nodes.
 *
 * <ol>
 *   <li>Evictable nodes are all placed in nodeCache, which prepares the nodes be selected by cache
 *       eviction.
 *   <li>None evictable nodes takes two parts:
 *       <ol>
 *         <li>The volatile nodes, new added or updated, which means the data has not been synced to
 *             disk.
 *         <li>The ancestors of the volatile nodes.
 *       </ol>
 * </ol>
 */
public interface IMemoryManager {

  void initRootStatus(ICachedMNode root);

  void updateCacheStatusAfterMemoryRead(ICachedMNode node) throws MNodeNotCachedException;

  void updateCacheStatusAfterDiskRead(ICachedMNode node);

  void updateCacheStatusAfterAppend(ICachedMNode node);

  void updateCacheStatusAfterUpdate(ICachedMNode node);

  IDatabaseMNode<ICachedMNode> collectUpdatedStorageGroupMNodes();

  /**
   * The returned node by iterator will automatically take the write lock. Please unlock the node
   * after process.
   */
  Iterator<ICachedMNode> collectVolatileSubtrees();

  /**
   * The returned node by iterator will automatically take the write lock. Please unlock the node
   * after process.
   */
  Iterator<ICachedMNode> updateCacheStatusAndRetrieveSubtreeAfterPersist(ICachedMNode subtreeRoot);

  void updateCacheStatusAfterFlushFailure(ICachedMNode subtreeRoot);

  void remove(ICachedMNode node);

  boolean evict();

  void pinMNode(ICachedMNode node) throws MNodeNotPinnedException;

  boolean unPinMNode(ICachedMNode node);

  long getBufferNodeNum();

  long getCacheNodeNum();

  void clear(ICachedMNode root);
}
