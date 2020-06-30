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

import scala.collection.JavaConverters._

import net.razorvine.pickle.{Pickler, Unpickler}
import org.graalvm.polyglot._

import org.apache.spark.{GraalEnv, InterruptibleIterator, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * A physical plan that evaluates a [[PythonUDF]]
 */
case class BatchEvalPythonExec(udfs: Seq[PythonUDF], resultAttrs: Seq[Attribute], child: SparkPlan)
  extends EvalPythonExec {

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    EvaluatePython.registerPicklers()  // register pickler for Row

    val dataTypes = schema.map(_.dataType)
    val needConversion = dataTypes.exists(EvaluatePython.needConversionInPython)

    // enable memo iff we serialize the row with schema (schema and class should be memorized)
    val pickle = new Pickler(needConversion)
    // Input iterator to Python: input rows are grouped so we send them in batches to Python.
    // For each row, add it to the queue.
    val inputIterator = iter.map { row =>
      if (needConversion) {
        EvaluatePython.toJava(row, schema)
      } else {
        // fast path for these types that does not need conversion in Python
        val fields = new Array[Any](row.numFields)
        var i = 0
        while (i < row.numFields) {
          val dt = dataTypes(i)
          fields(i) = EvaluatePython.toJava(row.get(i, dt), dt)
          i += 1
        }
        fields
      }
    }.grouped(100).map(x => pickle.dumps(x.toArray))

    // Output iterator for results from Python.
    if (context.getLocalProperty("spark.pyspark.pygraaludf.enabled") == "true") {
      val bytes =
        funcs(0).funcs(0).command.map(value => Integer.toUnsignedLong(value) & 0xff).toArray
      var start = System.currentTimeMillis()
      var end = 0L
      val graalContext = GraalEnv.graalContext.get()

      var test_udf: Value = null
      var func: Value = null
      var outputIterator: Iterator[InternalRow] = null

      graalContext.enter()
      end = System.currentTimeMillis()
      // scalastyle:off
      println("Get Context: " + (end - start) + "ms")
      import org.apache.spark.api.python.PythonUtils
      // scalastyle:off
      // println(PythonUtils.sparkPythonPath)

      start = System.currentTimeMillis()
      test_udf = graalContext.eval("python", "graalrunner.test_udf")
      end = System.currentTimeMillis()

      println("Get function(test_udf): " + (end - start) + "ms")
      start = System.currentTimeMillis()
      func = test_udf.execute(bytes)

      end = System.currentTimeMillis()
      println("Get function(myfunc): " + (end - start) + "ms")
      val runner = new PyGraalUDFRunner()
      val args = new Array[Object](argOffsets(0).size)

      start = System.currentTimeMillis()
      //      graalContext.enter()
      val mutableRow = new GenericInternalRow(1)
      outputIterator = iter.map { row =>
        var i = 0
        while (i < args.size) {
          val idx = argOffsets(0)(i)
          args(i) = row.get(idx, dataTypes(idx))
          i += 1
        }
        val result = runner.run(func, args)
        val fromJava = EvaluatePython.makeFromJava(udfs.head.dataType)
        mutableRow(0) = fromJava(result)
        mutableRow
      }
      graalContext.leave()

        //      graalContext.leave()
      end = System.currentTimeMillis()
      println("Execute function: " + (end - start) + "ms")
      new InterruptibleIterator[InternalRow](context, outputIterator)
//        val graalContext = SparkEnv.get.graalContext.get
        // val file = new File("graalrunner.py")
        // val source = Source.newBuilder("python", file).build
        // context.eval(source)
    } else {
      val outputIterator = new PythonUDFRunner(funcs, PythonEvalType.SQL_BATCHED_UDF, argOffsets)
        .compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val mutableRow = new GenericInternalRow(1)
      val resultType = if (udfs.length == 1) {
        udfs.head.dataType
      } else {
        StructType(udfs.map(u => StructField("", u.dataType, u.nullable)))
      }

      val fromJava = EvaluatePython.makeFromJava(resultType)

      outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
      }.map { result =>
        if (udfs.length == 1) {
          // fast path for single UDF
          mutableRow(0) = fromJava(result)
          mutableRow
        } else {
          fromJava(result).asInstanceOf[InternalRow]
        }
      }
    }
  }
}
