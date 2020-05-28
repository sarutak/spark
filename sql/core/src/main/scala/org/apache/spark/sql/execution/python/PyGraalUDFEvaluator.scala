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

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions

class PyGraalUDFEvaluator(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]]) {
  def compute(
      inputIterator: Iterator[Array[Byte]],
      partitionIndex: Int,
      context: TaskContext): Iterator[Array[Byte]] = {

    // scalastyle:off
    // println("fugafuga" * 100)
    val resultIterator = new Iterator[Array[Byte]] {
      override def hasNext(): Boolean = {
        inputIterator.hasNext
      }

      override def next(): Array[Byte] = {

      }
    }
    new InterruptibleIterator(context, resultIterator)
    Iterator(Array(1.asInstanceOf[Byte]))
  }
}
