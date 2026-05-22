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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Last
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.IntegerType

/**
 * Rewrites AS-OF Join (Backward, allowExactMatches=true, no tolerance, no residual beyond
 * equi-keys) to a Window-over-union plan:
 *
 * {{{
 *   -- 1. UNION ALL both sides with a source marker
 *   SELECT left_cols..., NULL as right_cols..., 0 as __src__ FROM left
 *   UNION ALL
 *   SELECT NULL as left_cols..., right_cols..., 1 as __src__ FROM right
 *
 *   -- 2. Window: last(right_struct) IGNORE NULLS
 *   last(struct(right_cols)) IGNORE NULLS
 *     OVER (PARTITION BY equi_keys ORDER BY as_of_key ASC, __src__ ASC
 *           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 *
 *   -- 3. Filter to left rows and non-null matches
 *   WHERE __src__ = 0 AND __right__ IS NOT NULL
 * }}}
 *
 * This rewrite is only applied when:
 * - spark.sql.join.windowRewriteAsOfJoin.enabled = true
 * - direction = Backward
 * - allowExactMatches = true (asOfCondition is GreaterThanOrEqual)
 * - no tolerance (asOfCondition has no And)
 * - condition contains only equi-join predicates (EqualTo)
 */
object RewriteAsOfJoinToWindow extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.windowRewriteAsOfJoinEnabled) return plan

    plan.transformUpWithNewOutput {
      case j @ AsOfJoin(left, right, asOfCondition, condition, joinType, _, _)
          if canRewrite(left, right, asOfCondition, condition) =>

        val (leftAsOf, rightAsOf) = extractAsOfKeys(asOfCondition)
        val equiKeys = extractEquiKeys(condition, left, right)

        // Source marker attribute
        val srcAttr = AttributeReference("__src__", IntegerType, nullable = false)()

        // Build left side of union: left_cols, NULL right_cols, src=0
        val leftNulls = right.output.map(a => Alias(Literal(null, a.dataType), a.name)())
        val leftProject = Project(
          left.output.map(a => a: NamedExpression) ++
            leftNulls :+
            Alias(Literal(0), "__src__")(),
          left)

        // Build right side of union: NULL left_cols, right_cols, src=1
        val rightNulls = left.output.map(a => Alias(Literal(null, a.dataType), a.name)())
        val rightProject = Project(
          rightNulls ++
            right.output.map(a => a: NamedExpression) :+
            Alias(Literal(1), "__src__")(),
          right)

        // Union
        val union = Union(Seq(leftProject, rightProject))

        // References in the union output
        val unionOutput = union.output
        val numLeft = left.output.size
        val numRight = right.output.size
        val unionLeftCols = unionOutput.take(numLeft)
        val unionRightCols = unionOutput.slice(numLeft, numLeft + numRight)
        val unionSrc = unionOutput.last

        // Resolve as-of key in union: leftAsOf references left's output,
        // map it to the corresponding union column
        val leftAsOfInUnion = mapExprToUnion(leftAsOf, left.output, unionLeftCols)
        val rightAsOfInUnion = mapExprToUnion(rightAsOf, right.output, unionRightCols)
        // The order key for the union: use Coalesce since one side is always NULL
        val asOfKeyInUnion = Coalesce(Seq(leftAsOfInUnion, rightAsOfInUnion))

        // Partition keys: equi-join keys mapped to union columns
        val partitionKeys = equiKeys.map { case (l, _) =>
          mapExprToUnion(l, left.output, unionLeftCols)
        }
        // For partition, we need Coalesce of left and right equi-keys
        val partitionExprs = equiKeys.map { case (l, r) =>
          Coalesce(Seq(
            mapExprToUnion(l, left.output, unionLeftCols),
            mapExprToUnion(r, right.output, unionRightCols)))
        }

        // Order: as_of_key ASC, src ASC (right=1 before left=0 would be wrong;
        // we want right rows BEFORE left rows at same ts, so src ASC means 0 < 1...
        // Wait: for backward, at same ts we want right to appear BEFORE left so that
        // last(right) picks it up. So right (src=1) should sort BEFORE left (src=0).
        // That means src DESC at same as-of key.
        // Actually: ORDER BY as_of_key ASC, src DESC
        // At ts=5: right row (src=1) sorts before left row (src=0) with DESC on src.
        // Then last(right_struct IGNORE NULLS) at the left row will see the right row.
        val orderSpec = Seq(
          SortOrder(asOfKeyInUnion, Ascending),
          SortOrder(unionSrc, Descending))

        // Window function: last(IF(src=1, struct(right_cols), NULL)) IGNORE NULLS
        // The struct must be NULL for left-side rows so IGNORE NULLS skips them.
        val rightStruct = CreateStruct(unionRightCols)
        val conditionalStruct = If(EqualTo(unionSrc, Literal(1)), rightStruct,
          Literal(null, rightStruct.dataType))
        val lastFunc = Last(conditionalStruct, ignoreNulls = true).toAggregateExpression()
        val windowSpec = WindowSpecDefinition(
          partitionExprs,
          orderSpec,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))
        val windowExpr = WindowExpression(lastFunc, windowSpec)
        val windowAlias = Alias(windowExpr, "__right__")()

        val window = Window(
          Seq(windowAlias),
          partitionExprs,
          orderSpec,
          union)

        // Filter to left-side rows
        val windowOutput = window.output
        val rightStructAttr = windowOutput.last // __right__ attribute
        val filterSrc = Filter(EqualTo(unionSrc, Literal(0)), window)

        // For Inner join, filter out null matches
        val filtered = joinType match {
          case LeftOuter => filterSrc
          case _ => Filter(IsNotNull(rightStructAttr), filterSrc)
        }

        // Final projection: original left cols + extracted right struct fields
        val finalProject = Project(
          unionLeftCols.map(a => a: NamedExpression) ++
            right.output.zipWithIndex.map { case (origAttr, idx) =>
              Alias(GetStructField(rightStructAttr, idx), origAttr.name)()
            },
          filtered)

        val attrMapping = j.output.zip(finalProject.output)
        finalProject -> attrMapping
    }
  }

  /** Check if this AsOfJoin can be rewritten to a window plan. */
  private def canRewrite(
      left: LogicalPlan,
      right: LogicalPlan,
      asOfCondition: Expression,
      condition: Option[Expression]): Boolean = {
    // Only Backward + allowExactMatches=true + no tolerance
    asOfCondition match {
      case GreaterThanOrEqual(l, r) =>
        // l references left, r references right
        l.references.subsetOf(left.outputSet) &&
          r.references.subsetOf(right.outputSet) &&
          isAllEquiJoin(condition, left, right)
      case _ => false
    }
  }

  /** Check that condition only contains EqualTo predicates between left and right. */
  private def isAllEquiJoin(
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan): Boolean = condition match {
    case None => true
    case Some(cond) =>
      splitConjunctivePredicates(cond).forall {
        case EqualTo(l, r) =>
          (l.references.subsetOf(left.outputSet) && r.references.subsetOf(right.outputSet)) ||
            (r.references.subsetOf(left.outputSet) && l.references.subsetOf(right.outputSet))
        case _ => false
      }
  }

  /** Extract (leftAsOf, rightAsOf) from the as-of condition. */
  private def extractAsOfKeys(asOfCondition: Expression): (Expression, Expression) = {
    asOfCondition match {
      case GreaterThanOrEqual(l, r) => (l, r)
      case _ => throw new IllegalStateException(s"Unexpected asOfCondition: $asOfCondition")
    }
  }

  /** Extract equi-join key pairs from condition. */
  private def extractEquiKeys(
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan): Seq[(Expression, Expression)] = condition match {
    case None => Seq.empty
    case Some(cond) =>
      splitConjunctivePredicates(cond).map {
        case EqualTo(l, r) if l.references.subsetOf(left.outputSet) => (l, r)
        case EqualTo(l, r) => (r, l) // swap so left is first
      }
  }

  /** Map an expression from one plan's output to the corresponding union columns. */
  private def mapExprToUnion(
      expr: Expression,
      fromOutput: Seq[Attribute],
      toOutput: Seq[Attribute]): Expression = {
    val mapping = fromOutput.zip(toOutput).toMap
    expr.transform {
      case a: AttributeReference => mapping.getOrElse(a, a)
    }
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(l, r) => splitConjunctivePredicates(l) ++ splitConjunctivePredicates(r)
      case other => Seq(other)
    }
  }
}
