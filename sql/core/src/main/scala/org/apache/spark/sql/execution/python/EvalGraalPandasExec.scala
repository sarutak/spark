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

package org.apache.spark.sql.execution.python

import org.graalvm.polyglot.Value

import org.apache.spark.{GraalEnv, InterruptibleIterator, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils

/**
 * A physical plan that evaluates a [[PythonUDF]].
 */
case class EvalGraalPandasExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends EvalPythonExec(udfs, resultAttrs, child) {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // DO NOT use iter.grouped(). See BatchIterator.
    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)
    val dataTypes = schema.map(_.dataType)

    val bytes =
      funcs(0).funcs(0).command.map(value => Integer.toUnsignedLong(value) & 0xff).toArray
    val graalContext = GraalEnv.graalContext.get

    var get_udf: Value = null
    var func: Value = null

    graalContext.enter()
    get_udf = graalContext.eval("python", "gr.get_pandas_scalar_udf")
    func = get_udf.execute(bytes)

    val runner = new GraalPythonUDFRunner()

    val mutableRow = new GenericInternalRow(1)
    val resultType = if (udfs.length == 1) {
      udfs.head.dataType
    } else {
      StructType(udfs.map(u => StructField("", u.dataType, u.nullable)))
    }

    val args = new Array[Object](argOffsets(0).size)
    val argBuffs = new Array[Value](argOffsets(0).size)

    argBuffs.indices.foreach(i => argBuffs(i) = graalContext.eval("python", "[]"))

    val fromGraal = EvaluateGraalPython.makeFromGraal(resultType)

    val outputIterator = batchIter.flatMap { batch =>
      batch.foreach { row =>
        var i = 0
        while (i < argOffsets(0).size) {
          val argIdx = argOffsets(0)(i)
          argBuffs(i).invokeMember("append", row.get(argIdx, dataTypes(argIdx)))
          i += 1
        }
      }
      var i = 0
      while (i < argOffsets(0).size) {
        args(i) = argBuffs(i)
        i += 1
      }
      val start = System.currentTimeMillis()
      val result = runner.run(func, args)
      val end = System.currentTimeMillis()

      // scalastyle:off
      // For debug
      println("Calculation: " + (end - start) + "ms")

      val resultSize = result.getMember("size").asLong
      val resultIter = new Iterator[InternalRow] {
        var count = 0L
        val iter = result.invokeMember("__iter__")
        override def hasNext(): Boolean = {
          count < resultSize
        }
        override def next(): InternalRow = {
          mutableRow(0) = fromGraal(iter.invokeMember("__next__"))
          count += 1
          mutableRow
        }
      }

      i = 0
      while(i < argOffsets(0).size) {
        argBuffs(i).invokeMember("clear")
        i += 1
      }
      resultIter
    }
    graalContext.leave()
    new InterruptibleIterator[InternalRow](context, outputIterator)
    // scalastyle:on
  }
}
