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

package org.apache.spark.sql.jdbc

import org.apache.spark.SparkFunSuite

/**
 * Helper class that runs docker integration tests.
 * Ignores them based on env variable is set or not.
 */
trait DockerIntegrationFunSuite extends SparkFunSuite {
  private val envVarNameForEnablingTests = "ENABLE_DOCKER_INTEGRATION_TESTS"
  private val shouldRunTests = sys.env.getOrElse(envVarNameForEnablingTests, "1") match {
    case "1" => true
    case _ => false
  }

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit): Unit = {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarNameForEnablingTests=1]")(testBody)
    }
  }

  /** Run the give body of code only if Kinesis tests are enabled */
  def runIfTestsEnabled(message: String)(body: => Unit): Unit = {
    if (shouldRunTests) {
      body
    } else {
      ignore(s"$message [enable by setting env var $envVarNameForEnablingTests=1]")(())
    }
  }
}
