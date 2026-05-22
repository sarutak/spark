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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LocalRelation, Window}
import org.apache.spark.sql.internal.SQLConf

class RewriteAsOfJoinToWindowSuite extends PlanTest {

  private def withWindowRewrite(f: => Unit): Unit = {
    withSQLConf(SQLConf.WINDOW_REWRITE_AS_OF_JOIN_ENABLED.key -> "true")(f)
  }

  test("backward + allowExactMatches + no tolerance + equi-key: rewrites to Window") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int, $"c".int)
      val right = LocalRelation($"d".int, $"e".int, $"f".int)
      val query = AsOfJoin(left, right, left.output(1), right.output(1),
        Some(left.output(0) === right.output(0)), Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      // Verify the rewrite produced a Window node (not a ScalarSubquery)
      assert(rewritten.find(_.isInstanceOf[Window]).isDefined,
        "Expected a Window node in the rewritten plan")
      // Verify no AsOfJoin remains
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isEmpty,
        "AsOfJoin should have been rewritten")
    }
  }

  test("backward + allowExactMatches + no tolerance + no equi-key: rewrites to Window") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      assert(rewritten.find(_.isInstanceOf[Window]).isDefined)
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isEmpty)
    }
  }

  test("left outer: rewrites without IsNotNull filter") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, LeftOuter,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      assert(rewritten.find(_.isInstanceOf[Window]).isDefined)
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isEmpty)
    }
  }

  test("forward direction: does NOT rewrite (falls through to RewriteAsOfJoin)") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("forward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      // Forward is not handled by the window rewrite
      assert(rewritten.find(_.isInstanceOf[Window]).isEmpty,
        "Forward direction should not be rewritten to Window")
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isDefined,
        "AsOfJoin should remain for forward direction")
    }
  }

  test("with tolerance: does NOT rewrite") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, Inner,
        tolerance = Some(10), allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      assert(rewritten.find(_.isInstanceOf[Window]).isEmpty)
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isDefined)
    }
  }

  test("allowExactMatches=false: does NOT rewrite") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, Inner,
        tolerance = None, allowExactMatches = false, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      assert(rewritten.find(_.isInstanceOf[Window]).isEmpty)
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isDefined)
    }
  }

  test("conf disabled: does NOT rewrite") {
    withSQLConf(SQLConf.WINDOW_REWRITE_AS_OF_JOIN_ENABLED.key -> "false") {
      val left = LocalRelation($"a".int, $"b".int)
      val right = LocalRelation($"c".int, $"d".int)
      val query = AsOfJoin(left, right, left.output(0), right.output(0),
        None, Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val rewritten = RewriteAsOfJoinToWindow(query.analyze)

      assert(rewritten.find(_.isInstanceOf[Window]).isEmpty)
      assert(rewritten.find(_.isInstanceOf[AsOfJoin]).isDefined)
    }
  }

  test("output schema matches original AsOfJoin output") {
    withWindowRewrite {
      val left = LocalRelation($"a".int, $"b".int, $"c".int)
      val right = LocalRelation($"d".int, $"e".int, $"f".int)
      val query = AsOfJoin(left, right, left.output(1), right.output(1),
        Some(left.output(0) === right.output(0)), Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

      val analyzed = query.analyze
      val rewritten = RewriteAsOfJoinToWindow(analyzed)

      assert(rewritten.output.map(_.name) === analyzed.output.map(_.name),
        "Output column names should match")
      assert(rewritten.output.map(_.dataType) === analyzed.output.map(_.dataType),
        "Output data types should match")
    }
  }
}
