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
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.io.Closeable

/**
 * Binary format visitor
 *
 * @param sft simple feature type
 * @param options encoding options
 */
class BinVisitor(sft: SimpleFeatureType, options: EncodingOptions) extends GeoMesaProcessVisitor with LazyLogging {

  import scala.collection.JavaConverters._

  // for collecting results manually
  private val manualResults = scala.collection.mutable.Queue.empty[Array[Byte]]
  private val manualConversion = BinaryOutputEncoder(sft, options)

  private var result: java.util.Iterator[Array[Byte]] = _

  override def getResult: BinResult = {
    if (result != null) {
      BinResult(result)
    } else {
      BinResult(manualResults.iterator.asJava)
    }
  }

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit =
    manualResults += manualConversion.encode(feature.asInstanceOf[SimpleFeature])

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")

    query.getHints.put(QueryHints.BIN_TRACK, options.trackIdField.map(sft.getDescriptor(_).getLocalName).getOrElse("id"))
    options.geomField.foreach(i => query.getHints.put(QueryHints.BIN_GEOM, sft.getDescriptor(i).getLocalName))
    options.dtgField.foreach(i => query.getHints.put(QueryHints.BIN_DTG, sft.getDescriptor(i).getLocalName))
    options.labelField.foreach(i => query.getHints.put(QueryHints.BIN_LABEL, sft.getDescriptor(i).getLocalName))

    val features =
      CloseableIterator(source.getFeatures(query).features()).map(_.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
    // try to ensure the iterator is closed, by making it Closeable (only discoverable via type matching), and by making it
    // auto-closing when fully read
    // TODO investigate options for returning Closeable things from a VectorProcess
    result = new java.util.Iterator[Array[Byte]] with Closeable {
      override def hasNext: Boolean = {
        val more = features.hasNext
        if (!more) {
          close()
        }
        more
      }
      override def next(): Array[Byte] = features.next()
      override def close(): Unit = features.close()
    }
  }
}

object BinVisitor {
  case class BinResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
    override def getValue: java.util.Iterator[Array[Byte]] = results
  }
}
