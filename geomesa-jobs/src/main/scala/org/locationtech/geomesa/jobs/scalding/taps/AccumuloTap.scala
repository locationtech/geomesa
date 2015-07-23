/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding.taps

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple.{Tuple, TupleEntryCollector, TupleEntryIterator, TupleEntrySchemeCollector}
import com.twitter.scalding.AccessMode
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.mapred.{AccumuloInputFormat, AccumuloOutputFormat, InputFormatBase}
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.jobs.mapred.InputFormatBaseAdapter
import org.locationtech.geomesa.jobs.scalding._

import scala.util.{Failure, Success, Try}

/**
 * Cascading Tap to read and write from accumulo
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
 */
case class AccumuloScheme(options: AccumuloSourceOptions)
    extends AccScheme(AccumuloSource.sourceFields, AccumuloSource.sinkFields) {

  import scala.collection.JavaConversions._

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf): Unit = {
    val input = Some(options).collect { case i: AccumuloInputOptions => i }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a source you must use AccumuloInputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (!ConfiguratorBase.isConnectorInfoSet(classOf[AccumuloInputFormat], conf)) {
      InputFormatBaseAdapter.setZooKeeperInstance(conf, input.instance, input.zooKeepers)
      InputFormatBaseAdapter.setConnectorInfo(conf, input.user, new PasswordToken(input.password.getBytes))
      InputFormatBase.setInputTableName(conf, input.table)
      InputFormatBaseAdapter.setScanAuthorizations(conf, input.authorizations)
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
      input.logLevel.foreach(InputFormatBaseAdapter.setLogLevel(conf, _))
    }

    conf.setInputFormat(classOf[AccumuloInputFormat])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf], tap: AccTap, conf: JobConf): Unit = {
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

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], MutOutputCollector]): Unit = {
    val entry = sc.getOutgoingEntry
    val table = entry.getObject(0).asInstanceOf[Text]
    val mutation = entry.getObject(1).asInstanceOf[Mutation]
    sc.getOutput.collect(table, mutation)
  }

  override def sourcePrepare(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]): Unit =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], KVRecordReader]): Unit =
    sc.setContext(null)
}

