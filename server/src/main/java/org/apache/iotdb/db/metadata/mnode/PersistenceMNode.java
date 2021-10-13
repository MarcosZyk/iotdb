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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.metadisk.cache.CacheEntry;
import org.apache.iotdb.db.metadata.metadisk.metafile.IPersistenceInfo;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.IOException;
import java.util.*;

public class PersistenceMNode implements IPersistenceInfo, IMNode {

  /** offset in metafile */
  private long[] positionList = new long[1];

  private int size = 0;

  public PersistenceMNode(long position) {
    size = 1;
    positionList[0] = position;
  }

  public PersistenceMNode(IPersistenceInfo persistenceInfo) {
    setPositionList(persistenceInfo.getPositionList());
  }

  @Override
  public long getStartPosition() {
    if (size == 0) {
      return 0;
    }
    return positionList[0];
  }

  @Override
  public List<Long> getPositionList() {
    List<Long> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add(positionList[i]);
    }
    return result;
  }

  @Override
  public void setPositionList(List<Long> positionList) {
    size = positionList.size();
    if (this.positionList.length < size) {
      this.positionList = new long[size];
    }
    for (int i = 0; i < size; i++) {
      this.positionList[i] = positionList.get(i);
    }
  }

  @Override
  public long get(int index) {
    return positionList[index];
  }

  @Override
  public void add(long value) {
    if (size == positionList.length) {
      positionList = Arrays.copyOf(positionList, 2 * size);
    }
    positionList[size] = value;
    size++;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public boolean isLoaded() {
    return false;
  }

  @Override
  public boolean isPersisted() {
    return true;
  }

  @Override
  public IPersistenceInfo getPersistenceInfo() {
    return this;
  }

  @Override
  public void setPersistenceInfo(IPersistenceInfo persistenceInfo) {
    if (persistenceInfo == null) {
      size = 0;
    } else {
      setPositionList(persistenceInfo.getPositionList());
    }
  }

  @Override
  public IMNode getEvictionHolder() {
    return this;
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public void addChild(String name, IMNode child) {}

  @Override
  public IMNode addChild(IMNode child) {
    return null;
  }

  @Override
  public void deleteChild(String name) {}

  @Override
  public void deleteAliasChild(String alias) {}

  @Override
  public IMNode getChild(String name) {
    return null;
  }

  @Override
  public int getMeasurementMNodeCount() {
    return 0;
  }

  @Override
  public boolean addAlias(String alias, IMNode child) {
    return false;
  }

  @Override
  public String getFullPath() {
    return null;
  }

  @Override
  public PartialPath getPartialPath() {
    return null;
  }

  @Override
  public IMNode getParent() {
    return null;
  }

  @Override
  public void setParent(IMNode parent) {}

  @Override
  public Map<String, IMNode> getChildren() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, IMNode> getAliasChildren() {
    return Collections.emptyMap();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {}

  @Override
  public void setAliasChildren(Map<String, IMNode> aliasChildren) {}

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void setName(String name) {}

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}

  @Override
  public void replaceChild(String measurement, IMNode newChildNode) {}

  @Override
  public boolean isUseTemplate() {
    return false;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {}

  @Override
  public Template getDeviceTemplate() {
    return null;
  }

  @Override
  public void setDeviceTemplate(Template deviceTemplate) {}

  @Override
  public Template getUpperTemplate() {
    return null;
  }

  @Override
  public CacheEntry getCacheEntry() {
    return null;
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {}

  @Override
  public boolean isCached() {
    return false;
  }

  @Override
  public void evictChild(String name) {}

  @Override
  public boolean isLockedInMemory() {
    return false;
  }

  @Override
  public boolean isDeleted() {
    return positionList[0] == -1;
  }

  @Override
  public IMNode clone() {
    return new PersistenceMNode(this);
  }
}
