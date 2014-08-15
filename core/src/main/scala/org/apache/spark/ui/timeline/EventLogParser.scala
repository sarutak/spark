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

package org.apache.spark.ui.timeline

import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex
import org.json4s._
import org.json4s.jackson._
import org.apache.spark.ui.timeline.dao._

/**
 * Event log parser
 */
private[spark] class EventLogParser(
  val filePath: String) {
  var taskInfos = new ListBuffer[TaskInfo]
  var executorIds = new ListBuffer[String]
  val reg = """.*"Event":"SparkListenerTaskEnd".*""".r

  def parse() : AppInfo = {
    read()
    new AppInfo(taskInfos.size, executorIds.sorted.distinct, taskInfos)
  }

  def read() {
    val lines = Source.fromFile(filePath).getLines()
    for (line <- lines) {
      line match {
        case reg() => createTaskInfo(line)
        case _ =>
      }
    }
  }

  def createTaskInfo(line: String) {
    val json = JsonMethods.parse(line).values

    val values = json.asInstanceOf[Map[String,Map[String,BigInt]]]
    val taskId = values("Task Info")("Task ID")
    val launchTime = new Date(values("Task Info")("Launch Time").toLong)
    val finishTime = new Date(values("Task Info")("Finish Time").toLong)
    val serilizationTime = values("Task Metrics")("Result Serialization Time")
    val deserializationTime =
      values("Task Metrics")("Executor Deserialize Time")

    val valuesStr = json.asInstanceOf[Map[String,Map[String,String]]]
    val executorId = valuesStr("Task Info")("Executor ID")
    executorIds += executorId

    var taskInfo = new TaskInfo(taskId,executorId,launchTime,finishTime,
      deserializationTime,serilizationTime)
    taskInfos += taskInfo
  }
}
