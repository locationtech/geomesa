/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.scalding.taps

import java.io.Closeable
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tuple._
import com.twitter.scalding.AccessMode
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriter, MultiTableBatchWriter, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.jobs.scalding._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Cascading Tap to read and write from accumulo in local mode
 */
case class AccumuloLocalTap(readOrWrite: AccessMode, scheme: AccumuloLocalScheme)
    extends AccLocalTap(scheme) with Logging {

  val options = scheme.options

  val getIdentifier: String = toString

  lazy val connector = new ZooKeeperInstance(options.instance, options.zooKeepers)
      .getConnector(options.user, new PasswordToken(options.password))
  lazy val tableOps = connector.tableOperations()

  override def openForRead(fp: FlowProcess[Properties], rr: KVRecordReader): TupleEntryIterator = {
    val input = options.asInstanceOf[AccumuloInputOptions]
    val scanner = if (input.ranges.isEmpty) {
      connector.createScanner(input.table, input.authorizations)
    } else {
      val bs = connector.createBatchScanner(input.table, input.authorizations, 5)
      bs.setRanges(input.ranges.flatMap(SerializedRangeSeq.unapply))
      bs
    }
    input.iterators.foreach(scanner.addScanIterator)
    input.columns.flatMap(SerializedColumnSeq.unapply)
        .foreach(p => scanner.fetchColumn(p.getFirst, p.getSecond))
    val entries = scanner.iterator()

    val iterator = new KVRecordReader() with Closeable {

      var pos: Int = 0

      override def next(key: Key, value: Value) = if (entries.hasNext) {
        val next = entries.next()
        key.set(next.getKey)
        value.set(next.getValue.get)
        pos += 1
        true
      } else {
        false
      }

      override def getProgress = 0f
      override def getPos = pos
      override def createKey() = new Key()
      override def createValue() = new Value()
      override def close() = scanner.close()
    }
    new TupleEntrySchemeIterator(fp, scheme, iterator)
  }


  override def openForWrite(fp: FlowProcess[Properties], out: MutOutputCollector): TupleEntryCollector = {
    val collector = new AccumuloLocalCollector(fp, this)
    collector.prepare()
    collector
  }

  override def createResource(conf: Properties): Boolean =
    Try(tableOps.create(options.table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error creating table ${options.table}", e)
        false
    }

  override def deleteResource(conf: Properties): Boolean =
    Try(tableOps.delete(options.table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error deleting table ${options.table}", e)
        false
    }

  override def resourceExists(conf: Properties): Boolean = tableOps.exists(options.table)

  override def getModifiedTime(conf: Properties): Long = System.currentTimeMillis()

  override def toString = s"AccumuloLocalTap[$readOrWrite,$options]"
}

/**
 * Collector that writes directly to accumulo
 */
class AccumuloLocalCollector(flowProcess: FlowProcess[Properties], tap: AccumuloLocalTap)
    extends TupleEntrySchemeCollector[Properties, MutOutputCollector](flowProcess, tap.getScheme)
    with MutOutputCollector {

  setOutput(this)

  private var writer: MultiTableBatchWriter = null
  private val writerCache = scala.collection.mutable.Map.empty[Text, BatchWriter]
  private val defaultTable = new Text(tap.options.table)

  override def prepare(): Unit = {
    val instance = new ZooKeeperInstance(tap.options.instance, tap.options.zooKeepers)
    val connector = instance.getConnector(tap.options.user, new PasswordToken(tap.options.password))
    writer = connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())
    sinkCall.setOutput(this)
    super.prepare()
  }

  override def close(): Unit = {
    writer.close()
    super.close()
  }

  override def collect(t: Text, m: Mutation): Unit = {
    val table = if (t == null) defaultTable else t
    val bw = writerCache.getOrElseUpdate(table, writer.getBatchWriter(t.toString))
    bw.addMutation(m)
  }
}

/**
 * Scheme to map between key value pairs and mutations
 */
case class AccumuloLocalScheme(options: AccumuloSourceOptions)
    extends AccLocalScheme(AccumuloSource.sourceFields, AccumuloSource.sinkFields) {

  override def sourceConfInit(fp: FlowProcess[Properties], tap: AccLocalTap, conf: Properties): Unit = {}

  override def sinkConfInit(fp: FlowProcess[Properties], tap: AccLocalTap, conf: Properties): Unit = {}

  override def source(fp: FlowProcess[Properties], sc: SourceCall[Array[Any], KVRecordReader]): Boolean = {
    val context = sc.getContext
    val k = context(0).asInstanceOf[Key]
    val v = context(1).asInstanceOf[Value]

    val hasNext = sc.getInput.next(k, v)
    if (hasNext) {
      sc.getIncomingEntry.setTuple(new Tuple(k, v))
    }
    hasNext
  }

  override def sink(fp: FlowProcess[Properties], sc: SinkCall[Array[Any], MutOutputCollector]): Unit = {
    val entry = sc.getOutgoingEntry
    val table = entry.getObject(0).asInstanceOf[Text]
    val mutation = entry.getObject(1).asInstanceOf[Mutation]
    sc.getOutput.collect(table, mutation)
  }

  override def sourcePrepare(fp: FlowProcess[Properties], sc: SourceCall[Array[Any], KVRecordReader]): Unit =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[Properties], sc: SourceCall[Array[Any], KVRecordReader]): Unit =
    sc.setContext(null)
}

