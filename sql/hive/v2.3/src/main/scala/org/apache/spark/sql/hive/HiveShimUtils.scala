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

import java.sql.{Date, Timestamp}
import java.util.{Base64, Properties}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
 * Various utilities for sql/hive used to upgrade the built-in Hive.
 */
private[hive] object HiveShimUtils {

  type TimestampWritable = org.apache.hadoop.hive.serde2.io.TimestampWritable
  type DateWritable = org.apache.spark.sql.execution.datasources.DaysWritable
  type Serializer = org.apache.hadoop.hive.serde2.Serializer
  type Deserializer = org.apache.hadoop.hive.serde2.Deserializer

  def fromTimestamp(t: Timestamp): Long = {
    DateTimeUtils.fromJavaTimestamp(t)
  }

  def fromDate(d: Date): Int = {
    DateTimeUtils.fromJavaDate(d)
  }

  def toTimestamp(t: Long): Timestamp = {
    DateTimeUtils.toJavaTimestamp(t)
  }

  def toDate(d: Int): Date = {
    DateTimeUtils.toJavaDate(d)
  }

  def getDeserializerClassFromTableDesc(tableDesc: TableDesc): Class[_ <: Deserializer] = {
    tableDesc.getDeserializerClass
  }

  def initializeSerializer(
      serializer: Serializer,
      conf: Configuration,
      props: Properties): Unit = {
    serializer.initialize(conf, props)
  }

  def initializeDeserializer(
      deserializer: Deserializer,
      conf: Configuration,
      props: Properties): Unit = {
    deserializer.initialize(conf, props)
  }

  // HIVE-11253 moved `toKryo` from `SearchArgument` to `storage-api` module.
  // This is copied from Hive 1.2's SearchArgumentImpl.toKryo().
  def sargToKryo(sarg: SearchArgument): String = {
    val kryo = new Kryo()
    val out = new Output(4 * 1024, 10 * 1024 * 1024)
    kryo.writeObject(out, sarg)
    out.close()
    Base64.getEncoder().encodeToString(out.toBytes)
  }

  def mkdir(fs: FileSystem, f: Path, conf: Configuration): Boolean = {
    FileUtils.mkdir(fs, f, true, conf)
  }
}
