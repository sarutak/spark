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

package org.apache.spark

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue

import org.graalvm.polyglot._

import org.apache.spark.api.python.PythonUtils

object GraalEnv {
  val graalContext = new ThreadLocal[Context] {
    override def initialValue(): Context = {
      createContext()
    }
  }

  val contextPool = new ConcurrentLinkedQueue[Context]()

  val lock = new Object

  private def createContext(): Context = {
    var engine: Engine = null
    var context: Context = null
    var start: Long = 0
    var end: Long = 0

    // Synchronize context initialization to avoid the weird race condition.
    // I have not yet investigate the reason but it sometimes happen without this synchronization.
    lock.synchronized {
      engine = Engine.create
      context = Context.newBuilder().allowAllAccess(true).engine(engine).build
      context.enter()

      var start = System.currentTimeMillis()
      context.eval("python", "import sys, site")
      var end = System.currentTimeMillis()

      // scalastyle:off
      // For debug
      println("import sys, site: " + (end - start) + "ms")

      val paths = PythonUtils.sparkPythonPath.split(File.pathSeparator)
        .map(path => "'" + path + "'").mkString("[", ",", "]")

      start = System.currentTimeMillis()
      context.eval("python", s"sys.path.extend($paths)")
      context.eval(
        "python",
        "site.addsitedir(sys.base_prefix + '/lib-python/' + " +
          "str(sys.version_info.major) + '/site-packages')")
      context.eval("python", "print(sys.base_prefix)")
      context.eval("python", "print(sys.version_info.major)")

      end = System.currentTimeMillis()
    }

    // For debug
    println("sys.path.extend: " + (end - start) + "ms")
    start = System.currentTimeMillis()
    context.eval("python", "import pyspark.graalrunner as gr, numpy as np")
    end = System.currentTimeMillis()

    // For debug
    println("from pyspark import graalrunner: " + (end - start) + "ms")
    context.leave()

    context
  }

  // Currently the following method is not used but leave it just in case.
  /*
  def initializeContextPool(num: Int): Unit = {
    contextPool.addAll((1 to num).par.map(_ => createContext).toList.asJava)
  }
  */
}
