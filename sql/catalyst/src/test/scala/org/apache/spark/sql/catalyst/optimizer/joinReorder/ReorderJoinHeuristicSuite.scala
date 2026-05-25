/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer.joinReorder

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for the heuristic-based join reordering in [[ReorderJoin]] when CBO is disabled.
 * The heuristic prefers:
 *   1. Equi-join conditions over non-equi conditions (enables hash join)
 *   2. Smaller tables (by sizeInBytes) when condition types are equal
 */
class ReorderJoinHeuristicSuite extends JoinReorderPlanTestBase with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) :: Nil
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConf(SQLConf.CBO_ENABLED, false)
    conf.setConf(SQLConf.STARSCHEMA_DETECTION, false)
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("a1") -> rangeColumnStat(10, 0),
    attr("a2") -> rangeColumnStat(10, 0),
    attr("b1") -> rangeColumnStat(10, 0),
    attr("b2") -> rangeColumnStat(10, 0),
    attr("c1") -> rangeColumnStat(10, 0),
    attr("c2") -> rangeColumnStat(10, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)

  // Small table (size = 100)
  private val small = StatsTestPlan(
    outputList = Seq("a1", "a2").map(nameToAttr),
    rowCount = 10,
    size = Some(100),
    attributeStats = AttributeMap(Seq("a1", "a2").map(n => nameToAttr(n) -> columnInfo(nameToAttr(n)))))

  // Medium table (size = 500)
  private val medium = StatsTestPlan(
    outputList = Seq("b1", "b2").map(nameToAttr),
    rowCount = 50,
    size = Some(500),
    attributeStats = AttributeMap(Seq("b1", "b2").map(n => nameToAttr(n) -> columnInfo(nameToAttr(n)))))

  // Large table (size = 1000)
  private val large = StatsTestPlan(
    outputList = Seq("c1", "c2").map(nameToAttr),
    rowCount = 100,
    size = Some(1000),
    attributeStats = AttributeMap(Seq("c1", "c2").map(n => nameToAttr(n) -> columnInfo(nameToAttr(n)))))

  test("prefer smaller table when multiple equi-join candidates exist") {
    // large JOIN medium JOIN small
    // conditions: large.c1 = small.a1 AND large.c2 = medium.b1
    // Both are equi-joins, so prefer smaller table (small: 100 < medium: 500)
    val query = large.join(medium).join(small)
      .where((nameToAttr("c1") === nameToAttr("a1")) &&
        (nameToAttr("c2") === nameToAttr("b1")))

    val expected = large
      .join(small, Inner, Some(nameToAttr("c1") === nameToAttr("a1")))
      .join(medium, Inner, Some(nameToAttr("c2") === nameToAttr("b1")))
      .select(outputsOf(large, medium, small): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("prefer equi-join over non-equi join") {
    // large JOIN medium JOIN small
    // conditions: large.c1 > small.a1 AND large.c2 = medium.b1
    // Equi-join (medium) preferred over non-equi (small)
    val query = large.join(medium).join(small)
      .where((nameToAttr("c1") > nameToAttr("a1")) &&
        (nameToAttr("c2") === nameToAttr("b1")))

    val optimized = Optimize.execute(query.analyze)
    // The innermost join should be equi-join (large ⋈ medium on c2=b1)
    // The outer join should be non-equi (... ⋈ small on c1>a1)
    val joins = optimized.collect {
      case j: org.apache.spark.sql.catalyst.plans.logical.Join => j
    }
    assert(joins.size == 2)
    // Outer join has the non-equi condition
    val outerJoin = joins.head
    assert(outerJoin.condition.get.find(_.isInstanceOf[
      org.apache.spark.sql.catalyst.expressions.GreaterThan]).isDefined)
  }

  test("equi-join preferred even when non-equi table is smaller") {
    // large JOIN small JOIN medium
    // conditions: large.c1 > small.a1 AND large.c2 = medium.b1
    // Even though small(100) < medium(500), equi-join with medium is preferred
    val query = large.join(small).join(medium)
      .where((nameToAttr("c1") > nameToAttr("a1")) &&
        (nameToAttr("c2") === nameToAttr("b1")))

    val optimized = Optimize.execute(query.analyze)
    // The innermost join should be equi-join (large ⋈ medium on c2=b1)
    val joins = optimized.collect {
      case j: org.apache.spark.sql.catalyst.plans.logical.Join => j
    }
    assert(joins.size == 2)
    // Outer join has the non-equi condition
    val outerJoin = joins.head
    assert(outerJoin.condition.get.find(_.isInstanceOf[
      org.apache.spark.sql.catalyst.expressions.GreaterThan]).isDefined)
  }
}
