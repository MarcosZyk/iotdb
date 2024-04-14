/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.RelationalPlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SimplifyExpressions;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.Statement;
import org.apache.iotdb.db.relational.sql.tree.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LogicalPlanner {
  private static final Logger LOG = Logger.get(LogicalPlanner.class);
  private final MPPQueryContext context;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator = new SymbolAllocator();
  private final List<RelationalPlanOptimizer> relationalPlanOptimizers;
  private final Metadata metadata;
  private final WarningCollector warningCollector;

  public LogicalPlanner(
      MPPQueryContext context,
      Metadata metadata,
      SessionInfo sessionInfo,
      WarningCollector warningCollector) {
    this.context = context;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");

    this.relationalPlanOptimizers = new ArrayList<>();
    this.relationalPlanOptimizers.add(new SimplifyExpressions());
  }

  public LogicalQueryPlan plan(Analysis analysis) throws IoTDBException {
    PlanNode planNode = planStatement(analysis, analysis.getStatement());

    relationalPlanOptimizers.forEach(optimizer -> optimizer.optimize(planNode, analysis, context));

    return new LogicalQueryPlan(context, planNode);
  }

  private PlanNode planStatement(Analysis analysis, Statement statement) throws IoTDBException {
    return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
  }

  private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
      throws IoTDBException {
    if (statement instanceof Query) {
      return createRelationPlan(analysis, (Query) statement);
    }
    throw new IoTDBException(
        "Unsupported statement type " + statement.getClass().getSimpleName(), -1);
  }

  private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();

    int columnNumber = 0;
    // TODO perfect the logic of outputDescriptor
    RelationType outputDescriptor = analysis.getOutputDescriptor();
    for (Field field : outputDescriptor.getVisibleFields()) {
      String name = field.getName().orElse("_col" + columnNumber);
      names.add(name);

      int fieldIndex = outputDescriptor.indexOf(field);
      Symbol symbol = plan.getSymbol(fieldIndex);
      outputs.add(symbol);

      columnNumber++;
    }

    return new OutputNode(
        context.getQueryId().genPlanNodeId(), plan.getRoot(), names.build(), outputs.build());
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(
        analysis, symbolAllocator, context.getQueryId(), sessionInfo, ImmutableMap.of());
  }

  private enum Stage {
    CREATED,
    OPTIMIZED,
    OPTIMIZED_AND_VALIDATED
  }
}
