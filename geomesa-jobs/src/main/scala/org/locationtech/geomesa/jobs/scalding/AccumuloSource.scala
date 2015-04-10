/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.jobs.scalding

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple._
import com.twitter.scalding._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.mapred.{AccumuloInputFormat, AccumuloOutputFormat, InputFormatBase}
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Range => AcRange, Value}
import org.apache.accumulo.core.util.{Pair => AcPair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.locationtech.geomesa.core.util.GeoMesaBatchWriterConfig

import scala.util.{Failure, Success, Try}

case class AccumuloSource(options: AccumuloSourceOptions)
    extends Source with TypedSource[(Key,Value)] with TypedSink[(Text, Mutation)] {

  def scheme = AccumuloScheme(options)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case Hdfs(_, _) => AccumuloTap(readOrWrite, scheme)
      case Test(_)    => TestTapFactory(this, scheme.asInstanceOf[GenericScheme]).createTap(readOrWrite)
      case _          => throw new NotImplementedError()
    }

  override def sourceFields: Fields = AccumuloSource.sourceFields

  override def converter[U >: (Key, Value)]: TupleConverter[U] = new TupleConverter[U] {
    override def arity: Int = 2
    override def apply(te: TupleEntry): (Key, Value) =
      (te.getObject(0).asInstanceOf[Key], te.getObject(1).asInstanceOf[Value])
  }

  override def sinkFields: Fields = AccumuloSource.sinkFields

  override def setter[U <: (Text, Mutation)]:  TupleSetter[U] = new TupleSetter[U] {
    override def arity: Int = 2
    override def apply(arg: U): Tuple = new Tuple(arg._1, arg._2)
  }
}

object AccumuloSource {
  def sourceFields: Fields = new Fields("k", "v")
  def sinkFields: Fields = new Fields("t", "m")
}

/**
 * Cascading Tap to read and write from accumulo
 *
 * @param readOrWrite
 * @param scheme
 */
case class AccumuloTap(readOrWrite: AccessMode, scheme: AccumuloScheme) extends AccTap(scheme) with Logging {

  val options = scheme.options

  val getIdentifier: String = toString

  lazy val tableOps = new ZooKeeperInstance(options.instance, options.zooKeepers)
                        .getConnector(options.user, new PasswordToken(options.password))
                        .tableOperations()

  override def openForRead(fp: FlowProcess[JobConf], rr: KVRecordReader): TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(fp, this, rr)

  override def openForWrite(fp: FlowProcess[JobConf], out: MutOutputCollector): TupleEntryCollector = {
    val collector = new AccumuloCollector(fp, this)
    collector.prepare()
    collector
  }

  override def createResource(conf: JobConf): Boolean =
    Try(tableOps.create(options.table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error creating table ${options.table}", e)
        false
    }

  override def deleteResource(conf: JobConf): Boolean =
    Try(tableOps.delete(options.table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error deleting table ${options.table}", e)
        false
    }

  override def resourceExists(conf: JobConf): Boolean = tableOps.exists(options.table)

  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  override def toString = s"AccumuloTap[$readOrWrite,$options]"
}

/**
 * Collector that writes to an AccumuloOutputFormat
 *
 * @param flowProcess
 * @param tap
 */
class AccumuloCollector(flowProcess: FlowProcess[JobConf], tap: AccumuloTap)
    extends TupleEntrySchemeCollector[JobConf, MutOutputCollector](flowProcess, tap.getScheme)
    with MutOutputCollector {

  val progress = flowProcess match {
    case process: HadoopFlowProcess => () => process.getReporter.progress()
    case _ => () => Unit
  }

  setOutput(this)

  private val conf = new JobConf(flowProcess.getConfigCopy)

  private var writer: MutRecordWriter = null

  override def prepare(): Unit = {
    tap.sinkConfInit(flowProcess, conf)
    val outputFormat = conf.getOutputFormat.asInstanceOf[AccumuloOutputFormat]
    writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier, Reporter.NULL)
    sinkCall.setOutput(this)
    super.prepare()
  }

  override def close(): Unit = {
    writer.close(Reporter.NULL)
    super.close()
  }

  override def collect(t: Text, m: Mutation): Unit = {
    progress()
    writer.write(t, m)
  }
}

/**
 * Scheme to map between key value pairs and mutations
 *
 * @param options
 */
case class AccumuloScheme(options: AccumuloSourceOptions)
    extends AccScheme(AccumuloSource.sourceFields, AccumuloSource.sinkFields) {

  import scala.collection.JavaConversions._

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf) {
    val input = Some(options).collect { case i: AccumuloInputOptions => i }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a source you must use AccumuloInputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (!ConfiguratorBase.isConnectorInfoSet(classOf[AccumuloInputFormat], conf)) {
      InputFormatBase.setZooKeeperInstance(conf, input.instance, input.zooKeepers)
      InputFormatBase.setConnectorInfo(conf, input.user, new PasswordToken(input.password.getBytes))
      InputFormatBase.setInputTableName(conf, input.table)
      InputFormatBase.setScanAuthorizations(conf, input.authorizations)
      if (input.ranges.nonEmpty) {
        val ranges = input.ranges.collect { case SerializedRangeSeq(r) => r }
        InputFormatBase.setRanges(conf, ranges)
      }
      if (input.columns.nonEmpty) {
        val cols = input.columns.collect { case SerializedColumnSeq(column) => column }
        InputFormatBase.fetchColumns(conf, cols)
      }
      input.iterators.foreach(InputFormatBase.addIterator(conf, _))
      input.autoAdjustRanges.foreach(InputFormatBase.setAutoAdjustRanges(conf, _))
      input.localIterators.foreach(InputFormatBase.setLocalIterators(conf, _))
      input.offlineTableScan.foreach(InputFormatBase.setOfflineTableScan(conf, _))
      input.scanIsolation.foreach(InputFormatBase.setScanIsolation(conf, _))
      input.logLevel.foreach(InputFormatBase.setLogLevel(conf, _))
    }

    conf.setInputFormat(classOf[AccumuloInputFormat])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf) {
    val output = Some(options).collect { case o: AccumuloOutputOptions => o }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a sink you must use AccumuloOutputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (!ConfiguratorBase.isConnectorInfoSet(classOf[AccumuloOutputFormat], conf)) {
      AccumuloOutputFormat.setConnectorInfo(
        conf, output.user, new PasswordToken(output.password.getBytes))
      AccumuloOutputFormat.setDefaultTableName(conf, output.table)
      AccumuloOutputFormat.setZooKeeperInstance(conf, output.instance, output.zooKeepers)
      val batchWriterConfig = GeoMesaBatchWriterConfig()
      output.threads.foreach(t => batchWriterConfig.setMaxWriteThreads(t))
      output.memory.foreach(m => batchWriterConfig.setMaxMemory(m))
      AccumuloOutputFormat.setBatchWriterOptions(conf, batchWriterConfig)
      AccumuloOutputFormat.setCreateTables(conf, output.createTable)
      output.logLevel.foreach(l => AccumuloOutputFormat.setLogLevel(conf, l))
    }
    conf.setOutputFormat(classOf[AccumuloOutputFormat])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Mutation])
  }

  override def source(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]): Boolean = {
    val context = sc.getContext
    val k = context(0).asInstanceOf[Key]
    val v = context(1).asInstanceOf[Value]

    val hasNext = sc.getInput.next(k, v)
    if (hasNext) {
      sc.getIncomingEntry.setTuple(new Tuple(k, v))
    }
    hasNext
  }

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], MutOutputCollector]) {
    val entry = sc.getOutgoingEntry
    val table = entry.getObject(0).asInstanceOf[Text]
    val mutation = entry.getObject(1).asInstanceOf[Mutation]
    sc.getOutput.collect(table, mutation)
  }

  override def sourcePrepare(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]) =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]) =
    sc.setContext(null)
}
