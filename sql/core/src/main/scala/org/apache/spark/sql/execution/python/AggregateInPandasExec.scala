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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.graalvm.polyglot.Value

import org.apache.spark.{GraalEnv, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * Physical node for aggregation with group aggregate Pandas UDF.
 *
 * This plan works by sending the necessary (projected) input grouped data as Arrow record batches
 * to the python worker, the python worker invokes the UDF and sends the results to the executor,
 * finally the executor evaluates any post-aggregation expressions and join the result with the
 * grouped key.
 */
case class AggregateInPandasExec(
    groupingExpressions: Seq[NamedExpression],
    udfExpressions: Seq[PythonUDF],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode {

  override val output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingExpressions.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val useGraalPython =
      SparkEnv.get.conf.get("spark.pyspark.graalpython.enabled", "false").toBoolean
    val inputRDD = child.execute()

    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (allInputs.exists(_.semanticEquals(e))) {
          allInputs.indexWhere(_.semanticEquals(e))
        } else {
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1
        }
      }.toArray
    }.toArray

    // Schema of input rows to the python runner
    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    })

    // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) iter else {
      val prunedProj = UnsafeProjection.create(allInputs, child.output)

      val grouped = if (groupingExpressions.isEmpty) {
        // Use an empty unsafe row as a place holder for the grouping key
        Iterator((new UnsafeRow(), iter))
      } else {
        GroupedIterator(iter, groupingExpressions, child.output)
      }.map { case (key, rows) =>
        (key, rows.map(prunedProj))
      }

      val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), groupingExpressions.length)
      context.addTaskCompletionListener[Unit] { _ =>
        queue.close()
      }

      // Add rows to queue to join later with the result.
      val projectedRowIter = grouped.map { case (groupingKey, rows) =>
        queue.add(groupingKey.asInstanceOf[UnsafeRow])
        rows
      }

      val columnarBatchIter = if (useGraalPython) {
        val dataTypes = aggInputSchema.map(_.dataType)
        val bytes =
          pyFuncs(0).funcs(0).command.map(value => Integer.toUnsignedLong(value) & 0xff).toArray
        val graalContext = GraalEnv.graalContext.get()
        val get_udf = graalContext.eval("python", "gr.get_pandas_grouped_agg_udf")
        val func = get_udf.execute(bytes)

        val runner = new GraalPythonUDFRunner

        val mutableRow = new GenericInternalRow(1)
        val resultType = if (udfExpressions.length == 1) {
          udfExpressions.head.dataType
        } else {
          StructType(udfExpressions.map(u => StructField("", u.dataType, u.nullable)))
        }

        // scalastyle: off
        // For debug
        // println("resultType is " + resultType)

        val args = new Array[Object](argOffsets(0).size)
        val argBuffs = new Array[Value](argOffsets(0).size)
        argBuffs.indices.foreach(i => argBuffs(i) = graalContext.eval("python", "[]"))
        val fromGraal = EvaluateGraalPython.makeFromGraal(resultType)
        projectedRowIter.map { rows =>
          rows.foreach { row =>
            var i = 0
            while(i < argOffsets(0).size) {
              val argIdx = argOffsets(0)(i)

              // For debug
              // println(row.get(argIdx, dataTypes(argIdx)))
              // println(row.get(argIdx, dataTypes(argIdx)).getClass)

              argBuffs(i).invokeMember("append", row.get(argIdx, dataTypes(argIdx)))
              i += 1
            }
          }
          var i = 0
          while (i < argOffsets(0).size) {
            args(i) = argBuffs(i)
            i += 1
          }
          val result = runner.run(func, args)

          // For debug
          // println(result)

          mutableRow(0) = fromGraal(result)

          // For debug
          // println(mutableRow)

          mutableRow
          // scalastyle:off
        }
      } else {
        new ArrowPythonRunner(
          pyFuncs,
          PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
          argOffsets,
          aggInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf).compute(projectedRowIter, context.partitionId(), context)
          .map(_.rowIterator().next())
      }

      val joinedAttributes =
        groupingExpressions.map(_.toAttribute) ++ udfExpressions.map(_.resultAttribute)
      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(resultExpressions, joinedAttributes)

      columnarBatchIter.map { aggOutputRow =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, aggOutputRow)
        resultProj(joinedRow)
      }
    }}
  }
}
