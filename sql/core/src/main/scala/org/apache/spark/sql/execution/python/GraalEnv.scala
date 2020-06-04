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

import org.graalvm.polyglot._

import org.apache.spark.api.python.PythonUtils

object GraalEnv {
  val engine = Engine.create
  val graalContext = new ThreadLocal[Context] {
    override def initialValue(): Context = {
      val context = Context.newBuilder().allowAllAccess(true).engine(engine).build
      context.enter()
      val paths = PythonUtils.sparkPythonPath.split(File.pathSeparator)
        .map(path => "'" + path + "'").mkString("[", ",", "]")
      context.eval("python", "import sys")
      context.eval("python", s"sys.path.extend($paths)")
//      context.eval("python", "import pyspark")
      context.eval("python", "from pyspark import graalrunner")
//      context.eval("python", "print('hogehogehogehoge')")
      context.leave()
      context
    }
  }
}
