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
import org.apache.accumulo.core.data.{Key, Mutation, Value, Range => AcRange}
import org.apache.accumulo.core.util.{Pair => AcPair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.locationtech.geomesa.core.util.GeoMesaBatchWriterConfig

import scala.util.{Failure, Success, Try}

case class AccumuloSource(options: AccumuloSourceOptions) extends Source with Mappable[(Key,Value)] {

  val hdfsScheme = new AccumuloScheme(options).asInstanceOf[GenericScheme]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case Hdfs(_, _) => new AccumuloTap(readOrWrite, new AccumuloScheme(options))
      case Test(_) => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
      case _ => throw new NotImplementedError()
    }

  def converter[U >: (Key, Value)]: TupleConverter[U] = new TupleConverter[U] {
    override def arity: Int = 2
    override def apply(te: TupleEntry): (Key, Value) =
      (te.getObject(0).asInstanceOf[Key], te.getObject(1).asInstanceOf[Value])
  }
}

/**
 * Cascading Tap to read and write from accumulo
 *
 * @param readOrWrite
 * @param scheme
 */
class AccumuloTap(readOrWrite: AccessMode, scheme: AccumuloScheme) extends AccTap(scheme) with Logging {

  val options = scheme.options

  val getIdentifier: String = readOrWrite.toString + options.toString

  lazy val tableOps = new ZooKeeperInstance(options.instance, options.zooKeepers)
                        .getConnector(options.user, new PasswordToken(options.password))
                        .tableOperations()

  lazy val table = readOrWrite match {
    case Read => options.input.table
    case Write => options.output.table
  }

  override def openForRead(fp: FlowProcess[JobConf], rr: KVRecordReader): TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(fp, this, rr)

  override def openForWrite(fp: FlowProcess[JobConf], out: MutOutputCollector): TupleEntryCollector = {
    val collector = new AccumuloCollector(fp, this)
    collector.prepare()
    collector
  }

  override def createResource(conf: JobConf): Boolean =
    Try(tableOps.create(table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error creating table $table", e)
        false
    }

  override def deleteResource(conf: JobConf): Boolean =
    Try(tableOps.delete(table)) match {
      case Success(_) => true
      case Failure(e) =>
        logger.error(s"Error deleting table $table", e)
        false
    }

  override def resourceExists(conf: JobConf): Boolean = tableOps.exists(table)

  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()
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
    if (flowProcess.isInstanceOf[HadoopFlowProcess]) {
      flowProcess.asInstanceOf[HadoopFlowProcess].getReporter().progress()
    }
    writer.write(t, m)
  }
}

/**
 * Scheme to map between key value pairs and mutations
 *
 * @param options
 */
class AccumuloScheme(val options: AccumuloSourceOptions)
  extends AccScheme(new Fields("key", "value"), new Fields("mutation")) {

  import scala.collection.JavaConversions._

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf) {
    // this method may be called more than once so check to see if we've already configured
    if (!ConfiguratorBase.isConnectorInfoSet(classOf[AccumuloInputFormat], conf)) {
      InputFormatBase.setZooKeeperInstance(conf, options.instance, options.zooKeepers)
      InputFormatBase.setConnectorInfo(conf,
                                       options.user,
                                       new PasswordToken(options.password.getBytes()))
      InputFormatBase.setInputTableName(conf, options.input.table)
      InputFormatBase.setScanAuthorizations(conf, options.input.authorizations)
      if (!options.input.ranges.isEmpty) {
        val ranges = options.input.ranges.collect { case SerializedRangeSeq(ranges) => ranges }
        InputFormatBase.setRanges(conf, ranges)
      }
      if (!options.input.columns.isEmpty) {
        val cols = options.input.columns.collect { case SerializedColumnSeq(column) => column }
        InputFormatBase.fetchColumns(conf, cols)
      }
      options.input.iterators.foreach(InputFormatBase.addIterator(conf, _))
      options.input.autoAdjustRanges.foreach(InputFormatBase.setAutoAdjustRanges(conf, _))
      options.input.localIterators.foreach(InputFormatBase.setLocalIterators(conf, _))
      options.input.offlineTableScan.foreach(InputFormatBase.setOfflineTableScan(conf, _))
      options.input.scanIsolation.foreach(InputFormatBase.setScanIsolation(conf, _))
      options.input.logLevel.foreach(InputFormatBase.setLogLevel(conf, _))
    }

    conf.setInputFormat(classOf[AccumuloInputFormat])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf) {
    // this method may be called more than once so check to see if we've already configured
    if (!ConfiguratorBase.isConnectorInfoSet(classOf[AccumuloOutputFormat], conf)) {
      AccumuloOutputFormat.setConnectorInfo(
        conf, options.user, new PasswordToken(options.password.getBytes()))
      AccumuloOutputFormat.setDefaultTableName(conf, options.output.table)
      AccumuloOutputFormat.setZooKeeperInstance(conf, options.instance, options.zooKeepers)
      val batchWriterConfig = GeoMesaBatchWriterConfig()
      options.output.threads.foreach(t => batchWriterConfig.setMaxWriteThreads(t))
      options.output.memory.foreach(m => batchWriterConfig.setMaxMemory(m))
      AccumuloOutputFormat.setBatchWriterOptions(conf, batchWriterConfig)
      AccumuloOutputFormat.setCreateTables(conf, options.output.createTable)
      options.output.logLevel.foreach(l => AccumuloOutputFormat.setLogLevel(conf, l))
    }
    conf.setOutputFormat(classOf[AccumuloOutputFormat])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Mutation])
  }

  override def source(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]): Boolean = {
    val k = sc.getContext.apply(0)
    val v = sc.getContext.apply(1)

    val hasNext = sc.getInput.next(k.asInstanceOf[Key], v.asInstanceOf[Value])

    if (hasNext) {
      val res = new Tuple()
      res.add(k)
      res.add(v)
      sc.getIncomingEntry.setTuple(res)
    }

    hasNext
  }

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], MutOutputCollector]) {
    val entry = sc.getOutgoingEntry
    val m = entry.getObject("mutation").asInstanceOf[Mutation]
    sc.getOutput.collect(null, m)
  }

  override def sourcePrepare(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]) =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]) =
    sc.setContext(null)
}
