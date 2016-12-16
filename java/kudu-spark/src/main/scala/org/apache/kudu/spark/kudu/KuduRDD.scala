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
package org.apache.kudu.spark.kudu

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import org.apache.kudu.client._
import org.apache.kudu.{Type, client}

/**
  * A Resilient Distributed Dataset backed by a Kudu table.
  */
class KuduRDD(val kuduMaster: String,
              @transient val batchSize: Integer,
              @transient val projectedCols: Array[String],
              @transient val predicates: Array[client.KuduPredicate],
              @transient val table: KuduTable,
              @transient val kc: KuduContext,
              @transient val sc: SparkContext) extends RDD[Row](sc, Nil) {

  /**
    * The [[KuduContext]] for this `KuduRDD`.
    *
    * The `KuduContext` manages the Kudu client instances for the `KuduRDD`.
    * When the `KuduRDD` is first constructed it uses the context passed in as
    * `kc`. After deserialization, a new `KuduContext` is created as necessary.
    * The `kc` field should not be used, since it will not be rehydrated after
    * serialization.
    */
  @transient private lazy val kuduContext: KuduContext = {
    if (kc != null) kc else new KuduContext(kuduMaster)
  }

  override protected def getPartitions: Array[Partition] = {
    val builder = kuduContext.syncClient
                             .newScanTokenBuilder(table)
                             .batchSizeBytes(batchSize)
                             .setProjectedColumnNames(projectedCols.toSeq.asJava)
                             .cacheBlocks(true)

    for (predicate <- predicates) {
      builder.addPredicate(predicate)
    }
    val tokens = builder.build().asScala
    tokens.zipWithIndex.map {
      case (token, index) =>
        new KuduPartition(index, token.serialize(),
                          token.getTablet.getReplicas.asScala.map(_.getRpcHost).toArray)
    }.toArray
  }

  override def compute(part: Partition, taskContext: TaskContext): Iterator[Row] = {
    val client: KuduClient = kuduContext.syncClient
    val partition: KuduPartition = part.asInstanceOf[KuduPartition]
    val scanner = KuduScanToken.deserializeIntoScanner(partition.scanToken, client)
    new RowResultIteratorScala(scanner)
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[KuduPartition].locations
  }
}

/**
  * A Spark SQL [[Partition]] which wraps a [[KuduScanToken]].
  */
private[spark] class KuduPartition(val index: Int,
                                   val scanToken: Array[Byte],
                                   val locations: Array[String]) extends Partition {}

/**
  * A Spark SQL [[Row]] iterator which wraps a [[KuduScanner]].
  * @param scanner the wrapped scanner
  */
private[spark] class RowResultIteratorScala(private val scanner: KuduScanner) extends Iterator[Row] {

  private var currentIterator: RowResultIterator = null

  override def hasNext: Boolean = {
    if ((currentIterator != null && !currentIterator.hasNext && scanner.hasMoreRows) ||
      (scanner.hasMoreRows && currentIterator == null)) {
      currentIterator = scanner.nextRows()
    }
    currentIterator.hasNext
  }

  override def next(): Row = {
    val rowResult = currentIterator.next()
    val length = rowResult.getColumnProjection.getColumnCount
    val array = new Array[Any](length)
    var i = 0

    while (i < length) {
      array(i) = 
        if (rowResult.isNull(i)) null
        else rowResult.getColumnType(i) match {
          case Type.BOOL => rowResult.getBoolean(i)
          case Type.INT8 => rowResult.getByte(i)
          case Type.INT16 => rowResult.getShort(i)
          case Type.INT32 => rowResult.getInt(i)
          case Type.INT64 => rowResult.getLong(i)
          case Type.UNIXTIME_MICROS => KuduRelation.microsToTimestamp(rowResult.getLong(i))
          case Type.FLOAT => rowResult.getFloat(i)
          case Type.DOUBLE => rowResult.getDouble(i)
          case Type.STRING => rowResult.getString(i)
          case Type.BINARY => rowResult.getBinaryCopy(i)
        }
        i += 1
    }
    new KuduRow(array)
  }
}

/**
  * A Spark SQL [[Row]] which wraps a Kudu [[RowResult]].
  * @param values the row result represented as array.
  */
private[spark] class KuduRow(private val values: Array[Any]) extends Row {
  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def copy(): Row = Row.fromSeq(Range(0, length).map(get))
}
