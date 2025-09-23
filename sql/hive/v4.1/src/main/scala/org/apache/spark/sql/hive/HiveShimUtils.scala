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

package org.apache.spark.sql.hive

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.ZoneOffset.UTC
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.common.`type`.{Date, Timestamp}
import org.apache.hadoop.hive.ql.io.sarg.{ConvertAstToSearchArg, SearchArgument}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.catalyst.util._

/**
 * Various utilities for sql/hive used to upgrade the built-in Hive.
 */
private[hive] object HiveShimUtils {

  type TimestampWritable = org.apache.hadoop.hive.serde2.io.TimestampWritableV2
  type DateWritable = org.apache.hadoop.hive.serde2.io.DateWritableV2
  type Serializer = org.apache.hadoop.hive.serde2.AbstractSerDe
  type Deserializer = org.apache.hadoop.hive.serde2.AbstractSerDe

  private val zoneId = ZoneId.systemDefault()

  def fromTimestamp(t: Timestamp): Long = {
    var localDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochSecond(t.toEpochSecond, t.getNanos), UTC)
    val julianDate =
      RebaseDateTime.rebaseJulianToGregorianDays(localDateTime.toLocalDate.toEpochDay.toInt)
    localDateTime = LocalDateTime.of(LocalDate.ofEpochDay(julianDate), localDateTime.toLocalTime)
    SparkDateTimeUtils.instantToMicros(
      localDateTime.toInstant(zoneId.getRules.getOffset(localDateTime)))
  }

  def fromDate(d: Date): Int = {
    d.toEpochDay
  }

  def toTimestamp(t: Long): Timestamp = {
    val javaTimestamp = DateTimeUtils.toJavaTimestamp(t)
    val hiveTimestamp = new Timestamp(javaTimestamp.toLocalDateTime)
    hiveTimestamp
  }

  def toDate(d: Int): Date = {
    Date.ofEpochDay(d)
  }

  def getDeserializerClassFromTableDesc(tableDesc: TableDesc): Class[_ <: Deserializer] = {
    tableDesc.getSerDeClass
  }

  def initializeSerializer(
      serializer: Serializer,
      conf: Configuration,
      props: Properties): Unit = {
    serializer.initialize(conf, props, null)
  }

  def initializeDeserializer(
      deserializer: Deserializer,
      conf: Configuration,
      props: Properties): Unit = {
    deserializer.initialize(conf, props, null)
  }

  def sargToKryo(sarg: SearchArgument): String = {
    ConvertAstToSearchArg.sargToKryo(sarg)
  }

  def mkdir(fs: FileSystem, f: Path, conf: Configuration): Boolean = {
    FileUtils.mkdir(fs, f, conf)
  }
}
