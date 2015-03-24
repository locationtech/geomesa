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

package org.locationtech.geomesa.jobs.scalding

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple._
import com.twitter.scalding._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.locationtech.geomesa.feature.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapred.{GeoMesaInputFormat, GeoMesaOutputFormat}
import org.opengis.feature.simple.SimpleFeature

/**
 * Source or sink for accessing GeoMesa
 * 
 * @param options
 */
case class GeoMesaSource(options: GeoMesaSourceOptions) extends Source with Mappable[(Text, SimpleFeature)] {

  val hdfsScheme = new GeoMesaScheme(options).asInstanceOf[GenericScheme]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case Hdfs(_, _) => new GeoMesaTap(readOrWrite, new GeoMesaScheme(options))
      case Test(_)    => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
      case _          => throw new NotImplementedError()
    }

  def converter[U >: (Text, SimpleFeature)]: TupleConverter[U] = new TupleConverter[U] {
    override val arity: Int = 2
    override def apply(te: TupleEntry): (Text, SimpleFeature) =
      (te.getObject(1).asInstanceOf[Text], te.getObject(0).asInstanceOf[SimpleFeature])
  }
}

/**
 * Cascading Tap to read and write from GeoMesa
 *
 * @param readOrWrite
 * @param scheme
 */
class GeoMesaTap(readOrWrite: AccessMode, scheme: GeoMesaScheme) extends GMTap(scheme) with Logging {

  val options = scheme.options

  val getIdentifier: String = readOrWrite.toString + options.toString

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
}

/**
 * Collector that writes to a GeoMesaOutputFormat
 *
 * @param flowProcess
 * @param tap
 */
class GeoMesaCollector(flowProcess: FlowProcess[JobConf], tap: GeoMesaTap)
    extends TupleEntrySchemeCollector[JobConf, GMOutputCollector](flowProcess, tap.getScheme)
    with GMOutputCollector {

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
    if (flowProcess.isInstanceOf[HadoopFlowProcess]) {
      flowProcess.asInstanceOf[HadoopFlowProcess].getReporter().progress()
    }
    writer.write(t, sf)
  }
}

/**
 * Scheme to map between tuples and simple features
 *
 * @param options
 */
class GeoMesaScheme(val options: GeoMesaSourceOptions)
  extends GMScheme(new Fields("id", "sf"), new Fields("id", "sf")) {

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: GMTap, conf: JobConf) {
    val input = Some(options).collect { case i: GeoMesaInputOptions => i }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a source you must use GeoMesaInputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (GeoMesaConfigurator.getDataStoreInParams(conf).isEmpty) {
      GeoMesaInputFormat.configure(conf, input.dsParams, input.feature, input.filter)
    }
    conf.setInputFormat(classOf[GeoMesaInputFormat])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf], tap: GMTap, conf: JobConf) {
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
    val k = sc.getContext.apply(0)
    val v = sc.getContext.apply(1)

    val hasNext = sc.getInput.next(k.asInstanceOf[Text], v.asInstanceOf[SimpleFeature])

    if (hasNext) {
      val res = new Tuple()
      res.add(k)
      res.add(v)
      sc.getIncomingEntry.setTuple(res)
    }

    hasNext
  }

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], GMOutputCollector]) {
    val entry = sc.getOutgoingEntry
    val id = entry.getObject("id").asInstanceOf[Text]
    val sf = entry.getObject("sf").asInstanceOf[SimpleFeature]
    sc.getOutput.collect(id, sf)
  }

  override def sourcePrepare(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], GMRecordReader]) =
    sc.setContext(Array(sc.getInput.createKey(), sc.getInput.createValue()))

  override def sourceCleanup(fp: FlowProcess[JobConf], sc: SourceCall[Array[Any], GMRecordReader]) =
    sc.setContext(null)
}

/**
 * Common trait for source/sink options
 */
sealed trait GeoMesaSourceOptions {
  def dsParams: Map[String, String]
}

/**
 * Options for configuring GeoMesa as a source
 */
case class GeoMesaInputOptions(dsParams: Map[String, String], feature: String, filter: Option[String] = None)
    extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaInputOptions[${dsParams.get("instanceId").getOrElse("None")}," +
      s"sft:$feature,filter:${filter.getOrElse("INCLUDE")}]"
}

/**
 * Options for configuring GeoMesa as a sink
 */
case class GeoMesaOutputOptions(dsParams: Map[String, String]) extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaOutputOptions[${dsParams.get("instanceId").getOrElse("None")}]"
}