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
case class GeoMesaSource(options: GeoMesaSourceOptions)
    extends Source with TypedSource[(Text, SimpleFeature)] with TypedSink[(Text, SimpleFeature)] {

  def scheme = GeoMesaScheme(options)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case Hdfs(_, _) => GeoMesaTap(readOrWrite, scheme)
      case Test(_)    => TestTapFactory(this, scheme.asInstanceOf[GenericScheme]).createTap(readOrWrite)
      case _          => throw new NotImplementedError()
    }
  override def sourceFields: Fields = GeoMesaSource.fields

  override def converter[U >: (Text, SimpleFeature)]: TupleConverter[U] = new TupleConverter[U] {
    override val arity: Int = 2
    override def apply(te: TupleEntry): (Text, SimpleFeature) =
      (te.getObject(0).asInstanceOf[Text], te.getObject(1).asInstanceOf[SimpleFeature])
  }

  override def sinkFields: Fields = GeoMesaSource.fields

  override def setter[U <: (Text, SimpleFeature)]:  TupleSetter[U] = new TupleSetter[U] {
    override def arity: Int = 2
    override def apply(arg: U): Tuple = new Tuple(arg._1, arg._2)
  }
}

object GeoMesaSource {
  def fields: Fields = new Fields("id", "sf")
}

/**
 * Cascading Tap to read and write from GeoMesa
 *
 * @param readOrWrite
 * @param scheme
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
 *
 * @param flowProcess
 * @param tap
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
 *
 * @param options
 */
case class GeoMesaScheme(options: GeoMesaSourceOptions)
  extends GMScheme(GeoMesaSource.fields, GeoMesaSource.fields) {

  override def sourceConfInit(fp: FlowProcess[JobConf], tap: GMTap, conf: JobConf) {
    val input = Some(options).collect { case i: GeoMesaInputOptions => i }.getOrElse(
      throw new IllegalArgumentException("In order to use this as a source you must use GeoMesaInputOptions")
    )

    // this method may be called more than once so check to see if we've already configured
    if (GeoMesaConfigurator.getDataStoreInParams(conf).isEmpty) {
      GeoMesaInputFormat.configure(conf, input.dsParams, input.feature, input.filter, input.transform)
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
    val context = sc.getContext
    val k = context(0).asInstanceOf[Text]
    val v = context(1).asInstanceOf[SimpleFeature]

    val hasNext = sc.getInput.next(k, v)
    if (hasNext) {
      sc.getIncomingEntry.setTuple(new Tuple(k, v))
    }
    hasNext
  }

  override def sink(fp: FlowProcess[JobConf], sc: SinkCall[Array[Any], GMOutputCollector]) {
    val entry = sc.getOutgoingEntry
    val id = entry.getObject(0).asInstanceOf[Text]
    val sf = entry.getObject(1).asInstanceOf[SimpleFeature]
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
case class GeoMesaInputOptions(dsParams: Map[String, String],
                               feature: String,
                               filter: Option[String] = None,
                               transform: Option[Array[String]] = None) extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaInputOptions[${dsParams.getOrElse("instanceId", "None")}," +
      s"${dsParams.getOrElse("tableName", "None")},$feature,${filter.getOrElse("INCLUDE")}]"
}

/**
 * Options for configuring GeoMesa as a sink
 */
case class GeoMesaOutputOptions(dsParams: Map[String, String]) extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaOutputOptions[${dsParams.getOrElse("instanceId", "None")}]"
}