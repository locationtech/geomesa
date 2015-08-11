/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding.taps

import java.io.Closeable
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.{SinkCall, SourceCall}
import cascading.tuple._
import com.twitter.scalding._
import org.apache.hadoop.io.Text
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Cascading Tap to read and write from GeoMesa in local mode
 */
case class GeoMesaLocalTap(readOrWrite: AccessMode, scheme: GeoMesaLocalScheme) extends GMLocalTap(scheme) {

  val getIdentifier: String = toString

  override def openForRead(fp: FlowProcess[Properties], rr: GMRecordReader): TupleEntryIterator = {
    val options = scheme.options.asInstanceOf[GeoMesaInputOptions]
    val ds = DataStoreFinder.getDataStore(options.dsParams).asInstanceOf[AccumuloDataStore]
    val cql = options.filter.map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val transform = options.transform.getOrElse(Query.ALL_NAMES)
    val query = new Query(options.feature, cql, transform)
    val reader = ds.getFeatureReader(options.feature, query)
    val sft = query.getHints.getTransformSchema.getOrElse(ds.getSchema(options.feature))

    val iterator = new GMRecordReader() with Closeable {

      var pos: Int = 0

      override def next(key: Text, value: SimpleFeature) = if (reader.hasNext) {
        val next = reader.next()
        key.set(next.getID)
        value.getIdentifier.asInstanceOf[FeatureIdImpl].setID(next.getID)
        value.setAttributes(next.getAttributes)
        value.getUserData.clear()
        value.getUserData.putAll(next.getUserData)
        pos += 1
        true
      } else {
        false
      }

      override def getProgress = 0f
      override def getPos = pos
      override def createKey() = new Text()
      override def createValue() = new ScalaSimpleFeature("", sft)
      override def close() = reader.close()
    }
    new TupleEntrySchemeIterator(fp, scheme, iterator)
  }

  override def openForWrite(fp: FlowProcess[Properties], out: GMOutputCollector): TupleEntryCollector = {
    val collector = new GeoMesaLocalCollector(fp, this)
    collector.prepare()
    collector
  }

  override def createResource(conf: Properties): Boolean = true

  override def deleteResource(conf: Properties): Boolean = true

  override def resourceExists(conf: Properties): Boolean = true

  override def getModifiedTime(conf: Properties): Long = System.currentTimeMillis()

  override def toString = s"GeoLocalMesaTap[$readOrWrite,${scheme.options}]"
}

/**
 * Collector that writes directly to GeoMesa
 */
class GeoMesaLocalCollector(flowProcess: FlowProcess[Properties], tap: GeoMesaLocalTap)
    extends TupleEntrySchemeCollector[Properties, GMOutputCollector](flowProcess, tap.getScheme)
    with GMOutputCollector {

  setOutput(this)

  private var ds: AccumuloDataStore = null
  private val writers = scala.collection.mutable.Map.empty[SimpleFeatureType, AccumuloFeatureStore]
  private val buffers = scala.collection.mutable.Map.empty[SimpleFeatureType, ArrayBuffer[SimpleFeature]]
  // we keep the buffer size fairly small since this is for local mode - mainly testing
  private val bufferSize = 100

  override def prepare(): Unit = {
    val options = tap.scheme.options.asInstanceOf[GeoMesaOutputOptions]
    ds = DataStoreFinder.getDataStore(options.dsParams).asInstanceOf[AccumuloDataStore]
    sinkCall.setOutput(this)
    super.prepare()
  }

  override def close(): Unit = {
    buffers.foreach { case (sft, features) => if (features.nonEmpty) { write(sft, features) } }
    super.close()
  }

  override def collect(t: Text, sf: SimpleFeature): Unit = {
    val sft = sf.getType
    val buffer = buffers.getOrElseUpdate(sft, ArrayBuffer.empty)
    buffer.append(sf)
    if (buffer.length >= bufferSize) {
      write(sft, buffer)
      buffer.clear()
    }
  }

  private def write(sft: SimpleFeatureType, features: Seq[SimpleFeature]): Unit = {
    val writer = writers.getOrElseUpdate(sft, {
      if (ds.getSchema(sft.getName) == null) {
        // this is a no-op if schema is already created, and should be thread-safe from different mappers
        ds.createSchema(sft)
        // short sleep to ensure that feature type is fully written if it is happening in some other thread
        Thread.sleep(5000)
      }
      ds.getFeatureSource(sft.getName).asInstanceOf[AccumuloFeatureStore]
    })
    writer.addFeatures(new ListFeatureCollection(sft, features))
  }
}

/**
 * Scheme to map between tuples and simple features
 */
case class GeoMesaLocalScheme(options: GeoMesaSourceOptions)
  extends GMLocalScheme(GeoMesaSource.fields, GeoMesaSource.fields) {

  override def sourceConfInit(fp: FlowProcess[Properties], tap: GMLocalTap, conf: Properties): Unit = {}

  override def sinkConfInit(fp: FlowProcess[Properties], tap: GMLocalTap, conf: Properties): Unit = {}

  override def source(fp: FlowProcess[Properties], sc: SourceCall[Array[Any], GMRecordReader]): Boolean = {
    val k = sc.getInput.createKey()
    val v = sc.getInput.createValue()
    val hasNext = sc.getInput.next(k, v)
    if (hasNext) {
      sc.getIncomingEntry.setTuple(new Tuple(k, v))
    }
    hasNext
  }

  override def sink(fp: FlowProcess[Properties], sc: SinkCall[Array[Any], GMOutputCollector]): Unit = {
    val entry = sc.getOutgoingEntry
    val id = entry.getObject(0).asInstanceOf[Text]
    val sf = entry.getObject(1).asInstanceOf[SimpleFeature]
    sc.getOutput.collect(id, sf)
  }
}
