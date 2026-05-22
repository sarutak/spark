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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.classic
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure AS-OF join performance: correlated subquery vs window-over-union rewrite.
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain
 *        org.apache.spark.sql.execution.benchmark.AsOfJoinBenchmark"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain
 *        org.apache.spark.sql.execution.benchmark.AsOfJoinBenchmark"
 *      Results will be written to "benchmarks/AsOfJoinBenchmark-results.txt".
 * }}}
 */
object AsOfJoinBenchmark extends SqlBasedBenchmark {

  private def doAsOfJoin(
      left: classic.DataFrame,
      right: classic.DataFrame,
      usingColumns: Seq[String]): Unit = {
    left.joinAsOf(
      right, left.col("ts"), right.col("ts"),
      usingColumns = usingColumns,
      joinType = "inner", tolerance = null,
      allowExactMatches = true, direction = "backward"
    ).noop()
  }

  private def asOfJoinBenchmark(
      leftRows: Int,
      rightRows: Int,
      numGroups: Int): Unit = {
    val left: classic.DataFrame = spark.range(leftRows).select(
      (col("id") % numGroups).as("group_id"),
      col("id").as("ts"),
      lit("left_val").as("left_val")
    ).toDF().asInstanceOf[classic.DataFrame]
    val right: classic.DataFrame = spark.range(rightRows).select(
      (col("id") % numGroups).as("group_id"),
      (col("id") * 3 / 2).as("ts"),
      lit("right_val").as("right_val")
    ).toDF().asInstanceOf[classic.DataFrame]

    val benchmark = new Benchmark(
      s"AS-OF Join (left=$leftRows, right=$rightRows, groups=$numGroups)",
      leftRows,
      output = output)

    benchmark.addCase("Correlated subquery (baseline)") { _ =>
      withSQLConf(
        SQLConf.WINDOW_REWRITE_AS_OF_JOIN_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        doAsOfJoin(left, right, Seq("group_id"))
      }
    }

    benchmark.addCase("Window-over-union rewrite") { _ =>
      withSQLConf(
        SQLConf.WINDOW_REWRITE_AS_OF_JOIN_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        doAsOfJoin(left, right, Seq("group_id"))
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("AS-OF Join Benchmark") {
      asOfJoinBenchmark(leftRows = 10000, rightRows = 10000, numGroups = 100)
    }
  }
}
