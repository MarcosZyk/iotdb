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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.StampedWriterPreferredLock;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LockManager {

  private final LockPool lockPool = new LockPool();

  public long stampedReadLock(ICachedMNode node) {
    return getMNodeLock(node).stampedReadLock();
  }

  public void stampedReadUnlock(ICachedMNode node, long stamp) {
    getMNodeLock(node).stampedReadUnlock(stamp);
  }

  public void threadReadLock(ICachedMNode node) {
    getMNodeLock(node).threadReadLock();
  }

  public void threadReadLock(ICachedMNode node, boolean prior) {
    getMNodeLock(node).threadReadLock(prior);
  }

  public void threadReadUnlock(ICachedMNode node) {
    getMNodeLock(node).threadReadUnlock();
  }

  public void writeLock(ICachedMNode node) {
    getMNodeLock(node).writeLock();
  }

  public void writeUnlock(ICachedMNode node) {
    getMNodeLock(node).writeUnlock();
  }

  private StampedWriterPreferredLock getMNodeLock(ICachedMNode node) {
    StampedWriterPreferredLock lock = node.getLock();
    if (lock == null) {
      lock = lockPool.borrowLock();
      node.setLock(lock);
    }
    return lock;
  }

  private void releaseMNodeLock(ICachedMNode node, StampedWriterPreferredLock lock) {
    if (lock.isFree()) {
      node.setLock(null);
      lockPool.returnLock(lock);
    }
  }

  private static class LockPool {
    private static final int LOCK_POOL_CAPACITY = 400;

    private final List<StampedWriterPreferredLock> lockList = new LinkedList<>();

    private final AtomicInteger activeLockNum = new AtomicInteger(0);

    private LockPool() {
      for (int i = 0; i < LOCK_POOL_CAPACITY; i++) {
        lockList.add(new StampedWriterPreferredLock());
      }
    }

    private StampedWriterPreferredLock borrowLock() {
      synchronized (lockList) {
        activeLockNum.getAndIncrement();
        if (lockList.isEmpty()) {
          return new StampedWriterPreferredLock();
        } else {
          return lockList.remove(0);
        }
      }
    }

    private void returnLock(StampedWriterPreferredLock lock) {
      synchronized (lockList) {
        activeLockNum.getAndDecrement();
        if (lockList.size() == LOCK_POOL_CAPACITY) {
          return;
        }
        lockList.add(0, lock);
      }
    }
  }
}
