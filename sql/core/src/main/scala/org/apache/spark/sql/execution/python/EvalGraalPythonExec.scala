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

/**
 * A physical plan that evaluates a [[PythonUDF]]
 */
case class EvalGraalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends EvalPythonExec(udfs, resultAttrs, child) {

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    // Maybe the following code is not necessary but leave and comment out for now just in case.
    // EvaluatePython.registerPicklers()  // register pickler for Row

    val dataTypes = schema.map(_.dataType)

    // Output iterator for results from Python.
    val bytes =
      funcs(0).funcs(0).command.map(value => Integer.toUnsignedLong(value) & 0xff).toArray
    var start = System.currentTimeMillis()
    var end = 0L
    val graalContext = GraalEnv.graalContext.get

    var get_udf: Value = null
    var func: Value = null
    var outputIterator: Iterator[InternalRow] = null

    graalContext.enter()
    end = System.currentTimeMillis()

    // scalastyle:off
    // For debug
    println("Get Context: " + (end - start) + "ms")

    start = System.currentTimeMillis()
    get_udf = graalContext.eval("python", "gr.get_udf")
    end = System.currentTimeMillis()

    // For debug
    println("Get function(get_udf): " + (end - start) + "ms")

    start = System.currentTimeMillis()
    func = get_udf.execute(bytes)
    end = System.currentTimeMillis()

    // For debug
    println("Get function(myfunc): " + (end - start) + "ms")

    val runner = new GraalPythonUDFRunner()
    val args = new Array[Object](argOffsets(0).size)

    start = System.currentTimeMillis()
    val mutableRow = new GenericInternalRow(1)

    val resultType = if (udfs.length == 1) {
      udfs.head.dataType
    } else {
      StructType(udfs.map(u => StructField("", u.dataType, u.nullable)))
    }

    val fromJava = EvaluateGraalPython.makeFromGraal(resultType)
    outputIterator = iter.map { row =>
      var i = 0
      while (i < args.size) {
        val idx = argOffsets(0)(i)
        args(i) = row.get(idx, dataTypes(idx))
        i += 1
      }
      val result = runner.run(func, args)
      mutableRow(0) = fromJava(result)
      mutableRow
    }
    graalContext.leave()

    end = System.currentTimeMillis()

    // For debug
    println("Execute function: " + (end - start) + "ms")

    new InterruptibleIterator[InternalRow](context, outputIterator)
    // scalastyle:on
  }
}
