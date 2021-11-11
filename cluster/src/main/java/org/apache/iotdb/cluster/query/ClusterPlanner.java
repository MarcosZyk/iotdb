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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterPlanner extends Planner {

  private MetaGroupMember metaGroupMember;
  private Coordinator coordinator;

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public PhysicalPlan parseSQLToPhysicalPlan(TSExecuteStatementReq req, ZoneId zoneId)
      throws QueryProcessException {
    String sqlStr = req.getStatement();
    Operator operator = logicalGenerator.generate(req.getStatement(), zoneId);
    if (operator instanceof QueryOperator && ((QueryOperator) operator).isGroupByTime()) {
      Map<String, String> sgPathMap = new HashMap<>();
      // split to different storage group
      for (PartialPath prefixPath : ((QueryOperator) operator).getFromOperator().getPrefixPaths()) {
        try {
          Map<String, String> tempMap = IoTDB.metaManager.determineStorageGroup(prefixPath);
          for (Map.Entry<String, String> temp : tempMap.entrySet()) {
            sgPathMap.putIfAbsent(temp.getKey(), temp.getValue());
          }
        } catch (IllegalPathException e) {
          throw new QueryProcessException(e);
        }
      }

      // traverse each storage group to create new sql
      Map<PartitionGroup, String> subQueryOperatorMap = new HashMap<>();
      int prefixStartIndex = sqlStr.indexOf("from") + 4;
      int groupByIndex = sqlStr.indexOf("group");
      String newSQL =
          sqlStr.substring(0, prefixStartIndex) + " %s " + sqlStr.substring(groupByIndex);
      for (Map.Entry<String, String> temp : sgPathMap.entrySet()) {
        String storageGroup = temp.getKey();
        PartitionGroup partitionGroup = metaGroupMember.getPartitionTable().route(storageGroup, 0);
        if (partitionGroup.contains(metaGroupMember.getThisNode())) {
          // local node
          try {
            ((QueryOperator) operator)
                .getFromOperator()
                .setPrefixList(Collections.singletonList(new PartialPath(temp.getValue())));
          } catch (IllegalPathException e) {
            throw new QueryProcessException(e);
          }
        } else {
          subQueryOperatorMap.put(partitionGroup, String.format(newSQL, temp.getValue()));
        }
      }

      // execute sub query operator remotely
      coordinator.forwardQuerySQL(subQueryOperatorMap, req.getSessionId(), req.getStatementId());
    }

    operator = logicalOptimize(operator);
    PhysicalGenerator physicalGenerator = new ClusterPhysicalGenerator();
    return physicalGenerator.transformToPhysicalPlan(operator, req.getFetchSize());
  }

  @Override
  protected ConcatPathOptimizer getConcatPathOptimizer() {
    return new ClusterConcatPathOptimizer();
  }
}
