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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Merges grouping aggregates that share identical GROUP BY keys and the same child plan
 * into a single aggregate stored as a CTE. Each original aggregate is replaced with a
 * Project selecting its columns from the shared CTE.
 *
 * Example:
 * {{{
 *   -- Before: two scans + two shuffles
 *   SELECT * FROM
 *     (SELECT b, sum(a) FROM t GROUP BY b) t1
 *     JOIN (SELECT b, max(a) FROM t GROUP BY b) t2 ON t1.b = t2.b
 *
 *   -- After: one scan + one shuffle (CTE shared)
 *   WITH merged AS (SELECT b, sum(a), max(a) FROM t GROUP BY b)
 *   SELECT * FROM
 *     (SELECT b, sum_a FROM merged) t1
 *     JOIN (SELECT b, max_a FROM merged) t2 ON t1.b = t2.b
 * }}}
 */
object MergeGroupingAggregates extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.MERGE_GROUPING_AGGREGATES_ENABLED)) return plan
    if (!conf.getConf(SQLConf.SUBQUERY_REUSE_ENABLED)) return plan
    plan match {
      case _: Subquery => plan
      case _ if plan.containsPattern(
        org.apache.spark.sql.catalyst.trees.TreePattern.CTE) => plan
      case _ => mergeGroupingAggregates(plan)
    }
  }

  private def mergeGroupingAggregates(plan: LogicalPlan): LogicalPlan = {
    // Collect all grouping aggregates in the plan
    val aggregates = mutable.ArrayBuffer.empty[(Aggregate, LogicalPlan)]
    collectGroupingAggregates(plan, aggregates)

    if (aggregates.size < 2) return plan

    // Group by canonicalized (grouping keys + child plan)
    val groups = aggregates.groupBy { case (agg, _) =>
      val key = (agg.groupingExpressions.map(_.canonicalized),
        agg.child.canonicalized)
      key
    }.values.filter(_.size >= 2).toSeq

    if (groups.isEmpty) return plan

    // For each group of mergeable aggregates, create a merged CTE
    var result = plan
    for (group <- groups) {
      result = mergeGroup(result, group.map(_._1).toSeq)
    }
    result
  }

  private def collectGroupingAggregates(
      plan: LogicalPlan,
      result: mutable.ArrayBuffer[(Aggregate, LogicalPlan)]): Unit = {
    plan match {
      case a: Aggregate if a.groupingExpressions.nonEmpty =>
        result += ((a, plan))
        // Don't recurse into the aggregate's child for finding more aggregates to merge
        // at this level - they would have different child plans
      case _ =>
        plan.children.foreach(collectGroupingAggregates(_, result))
    }
  }

  private def mergeGroup(plan: LogicalPlan, aggregates: Seq[Aggregate]): LogicalPlan = {
    val first = aggregates.head

    // Merge all aggregate expressions from all aggregates, deduplicating
    val allAggExprs = mutable.ArrayBuffer.empty[NamedExpression]
    val outputMappings = aggregates.map { agg =>
      agg.aggregateExpressions.map { expr =>
        val withoutAlias = expr match {
          case Alias(child, _) => child
          case e => e
        }
        val existingIdx = allAggExprs.indexWhere { existing =>
          val existingWithoutAlias = existing match {
            case Alias(child, _) => child
            case e => e
          }
          existingWithoutAlias.semanticEquals(withoutAlias)
        }
        if (existingIdx >= 0) {
          existingIdx
        } else {
          allAggExprs += expr
          allAggExprs.size - 1
        }
      }
    }

    // Create the merged aggregate
    val mergedAggregate = Aggregate(
      first.groupingExpressions,
      allAggExprs.toSeq,
      first.child
    )

    // Create a CTE definition
    val cteDef = CTERelationDef(mergedAggregate, underSubquery = false)
    val cteRef = CTERelationRef(cteDef.id, _resolved = true, mergedAggregate.output,
      mergedAggregate.isStreaming)

    // Replace each original aggregate with a Project over the CTE ref
    var result = plan
    for ((agg, mapping) <- aggregates.zip(outputMappings)) {
      result = result.transformUp {
        case node if node.fastEquals(agg) =>
          val projectList = mapping.zip(agg.output).map { case (mergedIdx, origAttr) =>
            Alias(cteRef.output(mergedIdx), origAttr.name)(origAttr.exprId)
          }
          Project(projectList, cteRef)
      }
    }

    // Wrap with WithCTE
    result match {
      case WithCTE(child, defs) => WithCTE(child, defs :+ cteDef)
      case _ => WithCTE(result, Seq(cteDef))
    }
  }
}
