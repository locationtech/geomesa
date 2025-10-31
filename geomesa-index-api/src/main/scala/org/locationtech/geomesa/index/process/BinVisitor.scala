/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, SimpleFeatureSource}
import org.geotools.api.feature.Feature
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.visitor.AbstractCalcResult
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.process.BinVisitor.BinResult
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodingOptions}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator


class BinVisitor(sft: SimpleFeatureType, options: EncodingOptions)
  extends GeoMesaProcessVisitor with LazyLogging {

  import scala.collection.JavaConverters._

  // for collecting results manually
  private val manualResults = scala.collection.mutable.Queue.empty[Array[Byte]]
  private val manualConversion = BinaryOutputEncoder(sft, options)

  private var result = new Iterator[Array[Byte]] {
    override def next(): Array[Byte] = manualResults.dequeue()
    override def hasNext: Boolean = manualResults.nonEmpty
  }

  override def getResult: BinResult = BinResult(result.asJava)

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit =
    manualResults += manualConversion.encode(feature.asInstanceOf[SimpleFeature])

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")

    query.getHints.put(QueryHints.BIN_TRACK, options.trackIdField.map(sft.getDescriptor(_).getLocalName).getOrElse("id"))
    options.geomField.foreach(i => query.getHints.put(QueryHints.BIN_GEOM, sft.getDescriptor(i).getLocalName))
    options.dtgField.foreach(i => query.getHints.put(QueryHints.BIN_DTG, sft.getDescriptor(i).getLocalName))
    options.labelField.foreach(i => query.getHints.put(QueryHints.BIN_LABEL, sft.getDescriptor(i).getLocalName))

    val features = SelfClosingIterator(source.getFeatures(query))
    result ++= features.map(_.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
  }
}

object BinVisitor {
  case class BinResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
    override def getValue: java.util.Iterator[Array[Byte]] = results
  }
}
