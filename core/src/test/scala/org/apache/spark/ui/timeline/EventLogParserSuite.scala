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

import scala.language.postfixOps
import org.scalatest.FunSuite
import java.io.{File, PrintWriter}
import java.util.Date

class EventLogParserSuite extends FunSuite {

  test("test read json file") {

    val task0 =
      """{"Event":"SparkListenerTaskEnd","Stage ID":1,
        |"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},
        |"Task Info":{"Task ID":3,"Index":1,"Launch Time":1407222233315,
        |"Executor ID":"1","Host":"343dcac379a9","Locality":"PROCESS_LOCAL",
        |"Getting Result Time":0,"Finish Time":1407222233364,"Failed":false,
        |"Serialized Size":0},"Task Metrics":{"Host Name":"343dcac379a9",
        |"Executor Deserialize Time":15,"Executor Run Time":11,
        |"Result Size":600,"JVM GC Time":0,"Result Serialization Time":0,
        |"Memory Bytes Spilled":0,"Disk Bytes Spilled":0}}""".stripMargin
        .replace("\n", "")
    val task1 =
      """{"Event":"SparkListenerTaskEnd","Stage ID":1,
        |"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},
        |"Task Info":{"Task ID":2,"Index":0,"Launch Time":1407222233313,
        |"Executor ID":"0","Host":"343dcac379a9","Locality":"PROCESS_LOCAL",
        |"Getting Result Time":0,"Finish Time":1407222233368,"Failed":false,
        |"Serialized Size":0},"Task Metrics":{"Host Name":"343dcac379a9",
        |"Executor Deserialize Time":23,"Executor Run Time":9,
        |"Result Size":600,"JVM GC Time":0,"Result Serialization Time":0,
        |"Memory Bytes Spilled":0,"Disk Bytes Spilled":0}}""".stripMargin
        .replace("\n", "")

    val filePath = "/tmp/event-log.json"
    val out = new PrintWriter(filePath)
    out.println(task0)
    out.println(task1)
    out.close

    val eventLogParser = new EventLogParser(filePath)

    try {
      val appInfo = eventLogParser.parse()
      assert(appInfo.taskNum == 2)
      assert(appInfo.executorIds.head == "0")

      val taskInfo0 = appInfo.taskInfoSet.apply(0)
      assert(taskInfo0.taskId == BigInt(3))
      assert(taskInfo0.executorId == "1")
      assert(taskInfo0.launchTime == new Date(1407222233315l))
      assert(taskInfo0.finishTime == new Date(1407222233364l))
      assert(taskInfo0.deserializationTime == BigInt(15))
      assert(taskInfo0.serializationTime == BigInt(0))

      val taskInfo1 = appInfo.taskInfoSet.apply(1)
      assert(taskInfo1.taskId == BigInt(2))
      assert(taskInfo1.executorId == "0")
      assert(taskInfo1.launchTime == new Date(1407222233313l))
      assert(taskInfo1.finishTime == new Date(1407222233368l))
      assert(taskInfo1.deserializationTime == BigInt(23))
      assert(taskInfo1.serializationTime == BigInt(0))

      val sortedByTaskId = appInfo.getTaskInfoSortByTaskId()
      assert(sortedByTaskId.apply(0).taskId == BigInt(2))
      assert(sortedByTaskId.apply(1).taskId == BigInt(3))

      val sortedByExecutorId = appInfo.getTaskInfoSortByExecutorId()
      assert(sortedByExecutorId.apply(0).executorId == "0")
      assert(sortedByExecutorId.apply(1).executorId == "1")

    } finally {
      val file = new File(filePath)
      file.delete()
    }
  }

}
