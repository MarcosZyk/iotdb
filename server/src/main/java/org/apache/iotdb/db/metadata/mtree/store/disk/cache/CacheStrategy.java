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
package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getBelongedContainer;
import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

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
 *         <li>The ancestors of the volatile nodes. If a node is not volatile but not in nodeCache,
 *             it means this node is an ancestor of some volatile node. Any volatile node is
 *             contained in the CachedContainer of its parent. The parent will be placed in
 *             nodeBuffer. If the parent is volatile as well, then the parent of the parent will be
 *             placed in nodeBuffer instead, which means the root node of a maximum volatile subtree
 *             will be placed in node buffer.
 *       </ol>
 * </ol>
 */
public class CacheStrategy implements ICacheStrategy {

  // The nodes in nodeCache are all evictable and may be selected to be evicted during cache
  // eviction.
  private volatile Map<CacheEntry, IMNode> nodeCache = new ConcurrentHashMap<>();

  // The nodeBuffer helps to quickly locate the volatile subtree
  private volatile Map<CacheEntry, IMNode> nodeBuffer = new ConcurrentHashMap<>();

  @Override
  public boolean isCached(IMNode node) {
    return node.getCacheEntry() != null;
  }

  /**
   * Some cache status of the given node may be updated based on concrete cache strategy after being
   * read in memory.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterMemoryRead(IMNode node) {
    if (nodeCache.containsKey(node.getCacheEntry())) {
      // todo update cache status for node selection during eviction
    }
  }

  /**
   * The node read from disk should be cached and added to nodeCache and the cache of its belonging
   * container.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterDiskRead(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    getBelongedContainer(node).addChildToCache(node);
    nodeCache.putIfAbsent(cacheEntry, node);
  }

  /**
   * The new appended node should be cached and the volatile subtree it belonged should be added to
   * nodeBuffer.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterAppend(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.setVolatile(true);
    getBelongedContainer(node).appendMNode(node);
    addNodeToBuffer(node);
  }

  /**
   * The updated node should be marked volatile and removed from nodeCache if necessary and the
   * volatile subtree it belonged should be added to nodeBuffer.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterUpdate(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (!cacheEntry.isVolatile()) {
      cacheEntry.setVolatile(true);
      getBelongedContainer(node).updateMNode(node.getName());
      synchronized (node) {
        nodeCache.remove(cacheEntry);
      }
      addNodeToBuffer(node);
    }
  }

  /**
   * look for the first none volatile ancestor of this node and add it to nodeBuffer. Through this
   * the volatile subtree the given node belong to will be record in nodeBuffer.
   *
   * @param node
   */
  private void addNodeToBuffer(IMNode node) {
    IMNode parent = node.getParent();
    CacheEntry cacheEntry;
    while (parent != null) {
      cacheEntry = parent.getCacheEntry();
      if (nodeCache.containsKey(cacheEntry)) {
        synchronized (parent) {
          if (nodeCache.containsKey(cacheEntry)) {
            // the ancestors of volatile node should not stay in nodeCache in which the node will be
            // evicted
            nodeCache.remove(cacheEntry);
            parent = parent.getParent();
          } else {
            break;
          }
        }
      } else {
        break;
      }
    }
    parent = node.getParent();
    cacheEntry = parent.getCacheEntry();
    if (!cacheEntry.isVolatile()) {
      // make sure that the nodeBuffer contains all the root node of volatile subTree
      // give that root.sg.d.s, if root, sg and d have been persisted and s are volatile, then d
      // will be added to nodeBuffer
      nodeBuffer.put(cacheEntry, parent);
    }
  }

  /**
   * After flush the given node's container to disk, the cache status of the nodes in container and
   * their ancestors should be updated. All the node should be added to nodeCache and the nodeBuffer
   * should be cleared after finishing a MTree flush task.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterPersist(IMNode node) {
    IMNode tmp = node;
    while (tmp.getParent() != null && !nodeCache.containsKey(tmp.getCacheEntry())) {
      nodeCache.put(tmp.getCacheEntry(), tmp);
      tmp = tmp.getParent();
    }
    ICachedMNodeContainer container = getCachedMNodeContainer(node);
    Map<String, IMNode> persistedChildren = container.getNewChildBuffer();
    for (IMNode child : persistedChildren.values()) {
      updateCacheStatusAfterPersist(child, container);
    }

    persistedChildren = container.getUpdatedChildBuffer();
    for (IMNode child : persistedChildren.values()) {
      updateCacheStatusAfterPersist(child, container);
    }
  }

  private void updateCacheStatusAfterPersist(IMNode node, ICachedMNodeContainer container) {
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.setVolatile(false);
    container.moveMNodeToCache(node.getName());
    nodeCache.put(cacheEntry, node);
  }

  /**
   * Collect nodes in all volatile subtrees. All volatile nodes' parents will be collected. The
   * ancestor will be in front of the descendent in returned list.
   *
   * @return
   */
  @Override
  public List<IMNode> collectVolatileMNodes() {
    List<IMNode> nodesToPersist = new ArrayList<>();
    for (IMNode node : nodeBuffer.values()) {
      collectVolatileNodes(node, nodesToPersist);
    }
    nodeBuffer.clear();
    return nodesToPersist;
  }

  private void collectVolatileNodes(IMNode node, List<IMNode> nodesToPersist) {
    CacheEntry cacheEntry;
    boolean isCollected = false;
    for (IMNode child : node.getChildren().values()) {
      cacheEntry = child.getCacheEntry();
      if (cacheEntry == null || !cacheEntry.isVolatile()) {
        continue;
      }
      if (!isCollected) {
        nodesToPersist.add(node);
        isCollected = true;
      }
      collectVolatileNodes(child, nodesToPersist);
    }
  }

  @Override
  public List<IMNode> remove(IMNode node) {
    List<IMNode> removedMNodes = new LinkedList<>();
    removeRecursively(node, removedMNodes);
    return removedMNodes;
  }

  private void removeOne(CacheEntry cacheEntry) {
    if (cacheEntry.isVolatile()) {
      nodeBuffer.remove(cacheEntry);
    } else {
      nodeCache.remove(cacheEntry);
    }
  }

  private void removeRecursively(IMNode node, List<IMNode> removedMNodes) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (cacheEntry == null) {
      return;
    }
    removeOne(cacheEntry);
    removedMNodes.add(node);
    for (IMNode child : node.getChildren().values()) {
      removeRecursively(child, removedMNodes);
    }
  }

  /**
   * Choose an evictable node from nodeCache and evicted all the cached node in the subtree it
   * represented.
   *
   * @return
   */
  @Override
  public synchronized List<IMNode> evict() {
    IMNode node = null;
    List<IMNode> evictedMNodes = new ArrayList<>();
    for (CacheEntry cacheEntry : nodeCache.keySet()) {
      if (!cacheEntry.isPinned()) {
        node = nodeCache.get(cacheEntry);
        // the operation that may change the cache status of a node should be synchronized
        synchronized (node) {
          if (!cacheEntry.isPinned() && nodeCache.containsKey(cacheEntry)) {
            getBelongedContainer(node).evictMNode(node.getName());
            nodeCache.remove(node.getCacheEntry());
            node.setCacheEntry(null);
            evictedMNodes.add(node);
            break;
          }
        }
      }
    }
    if (node != null) {
      collectEvictedMNodes(node, evictedMNodes);
    }
    return evictedMNodes;
  }

  private void collectEvictedMNodes(IMNode node, List<IMNode> evictedMNodes) {
    for (IMNode child : node.getChildren().values()) {
      nodeCache.remove(child.getCacheEntry());
      child.setCacheEntry(null);
      evictedMNodes.add(child);
      collectEvictedMNodes(child, evictedMNodes);
    }
  }

  /**
   * Pin a node in memory, and it will not be evicted. The pin is implemented as a multi mayer lock.
   * When a thread/task pin the node, one mayer lock will be added to the node and its parent. This
   * help guarantees the cache assumption that if a node is cached/pinned, its parent should be
   * cached/pinned.
   *
   * @param node
   */
  @Override
  public void pinMNode(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (cacheEntry == null) {
      cacheEntry = new CacheEntry();
      node.setCacheEntry(cacheEntry);
    }
    if (!cacheEntry.isPinned()) {
      IMNode parent = node.getParent();
      if (parent != null) {
        parent.getCacheEntry().pin();
      }
    }
    cacheEntry.pin();
  }

  /**
   * Unpin a node, and if the lock mayer on this node is zero, it will be evictable. Unpin a node
   * means decrease one mayer lock from the node. If the lock mayer reaches zero, the mayer lock of
   * its parent should decrease.
   *
   * @param node
   * @return
   */
  @Override
  public List<IMNode> unPinMNode(IMNode node) {
    List<IMNode> releasedMNodes = new ArrayList<>();
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.unPin();
    if (!cacheEntry.isPinned()) {
      releasedMNodes.add(node);
      IMNode parent = node.getParent();
      while (parent != null) {
        node = parent;
        parent = node.getParent();
        cacheEntry = node.getCacheEntry();
        cacheEntry.unPin();
        if (cacheEntry.isPinned()) {
          break;
        }
        releasedMNodes.add(node);
      }
    }
    return releasedMNodes;
  }

  @Override
  public boolean isPinned(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    return cacheEntry != null && cacheEntry.isPinned();
  }

  @Override
  public void clear() {
    nodeCache.clear();
    nodeBuffer.clear();
  }
}
