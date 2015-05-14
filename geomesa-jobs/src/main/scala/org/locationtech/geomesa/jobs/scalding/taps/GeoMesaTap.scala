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

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple._
import com.twitter.scalding._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapred.{GeoMesaInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeature

/**
 * Cascading Tap to read and write from GeoMesa
 */
case class GeoMesaTap(readOrWrite: AccessMode, scheme: GeoMesaScheme) extends GMTap(scheme) {

  val getIdentifier: String = toString

  override def openForRead(fp: FlowProcess[JobConf], rr: GMRecordReader): TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(fp, this, rr)

  override def openForWrite(fp: FlowProcess[JobConf], out: GMOutputCollector): TupleEntryCollector = {
    val collector = new GeoMesaCollector(fp, this)
    collector.prepare()
    collector
  }

  override def createResource(conf: JobConf): Boolean = true

  override def deleteResource(conf: JobConf): Boolean = true

  override def resourceExists(conf: JobConf): Boolean = true

  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  override def toString = s"GeoMesaTap[$readOrWrite,${scheme.options}]"
}

/**
 * Collector that writes to a GeoMesaOutputFormat
 */
class GeoMesaCollector(flowProcess: FlowProcess[JobConf], tap: GeoMesaTap)
    extends TupleEntrySchemeCollector[JobConf, GMOutputCollector](flowProcess, tap.getScheme)
    with GMOutputCollector {

  val progress = flowProcess match {
    case process: HadoopFlowProcess => () => process.getReporter.progress()
    case _ => () => Unit
  }

  setOutput(this)

  private val conf = new JobConf(flowProcess.getConfigCopy)

  private var writer: GMRecordWriter = null

  override def prepare(): Unit = {
    tap.sinkConfInit(flowProcess, conf)
    val outputFormat = conf.getOutputFormat.asInstanceOf[GeoMesaOutputFormat]
    writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier, Reporter.NULL)
    sinkCall.setOutput(this)
    super.prepare()
  }

  override def close(): Unit = {
    writer.close(Reporter.NULL)
    super.close()
  }

  override def collect(t: Text, sf: SimpleFeature): Unit = {
    progress()
    writer.write(t, sf)
  }
}

/**
 * Scheme to map between tuples and simple features
 */
case class GeoMesaScheme(options: GeoMesaSourceOptions)
  extends GMScheme(GeoMesaSource.fields, GeoMesaSource.fields) {

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: GMTap, conf: JobConf): Unit = {
    val input = Some(options).collect { case i: GeoMesaInputOptions => i }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a source you must use GeoMesaInputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (GeoMesaConfigurator.getDataStoreInParams(conf).isEmpty) {
      GeoMesaInputFormat.configure(conf, input.dsParams, input.feature, input.filter, input.transform)
    }
    conf.setInputFormat(classOf[GeoMesaInputFormat])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf], tap: GMTap, conf: JobConf): Unit = {
    val output = Some(options).collect { case o: GeoMesaOutputOptions => o }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a sink you must use GeoMesaOutputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (GeoMesaConfigurator.getDataStoreOutParams(conf).isEmpty) {
      GeoMesaOutputFormat.configureDataStore(conf, output.dsParams)
      // TODO support batch writing config?
      // GeoMesaOutputFormat.configureBatchWriter(conf, null)
    }
    conf.setOutputFormat(classOf[GeoMesaOutputFormat])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[ScalaSimpleFeature])
  }

  override def source(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], GMRecordReader]): Boolean = {
    val context = sc.getContext
    val k = context(0).asInstanceOf[Text]
    val v = context(1).asInstanceOf[SimpleFeature]

    val hasNext = sc.getInput.next(k, v)
    if (hasNext) {
      sc.getIncomingEntry.setTuple(new Tuple(k, v))
    }
    hasNext
  }

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], GMOutputCollector]): Unit = {
    val entry = sc.getOutgoingEntry
    val id = entry.getObject(0).asInstanceOf[Text]
    val sf = entry.getObject(1).asInstanceOf[SimpleFeature]
    sc.getOutput.collect(id, sf)
  }

  override def sourcePrepare(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], GMRecordReader]): Unit =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], GMRecordReader]): Unit =
    sc.setContext(null)
}
