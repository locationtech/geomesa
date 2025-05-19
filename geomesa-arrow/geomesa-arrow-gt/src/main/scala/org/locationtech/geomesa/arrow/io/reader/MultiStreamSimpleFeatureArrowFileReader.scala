/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.reader

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.InputStream
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Streaming reader that will re-read the input stream on each call to features()
 *
 * @param is input stream function
 */
class MultiStreamSimpleFeatureArrowFileReader(is: () => InputStream) extends SimpleFeatureArrowFileReader {

  private val reader = new StreamingSimpleFeatureArrowFileReader(is())
  private val createNewReader = new AtomicBoolean(false)

  override def sft: SimpleFeatureType = reader.sft
  override def dictionaries: Map[String, ArrowDictionary] = reader.dictionaries
  override def vectors: Seq[SimpleFeatureVector] = reader.vectors
  override def features(filter: Filter): CloseableIterator[ArrowSimpleFeature] = {
    // we can use the original reader for the first query, subsequent queries we have to create a new one
    val newReader =
      if (createNewReader.compareAndSet(false, true)) { None } else {
        Some(new StreamingSimpleFeatureArrowFileReader(is()))
      }
    val iter = newReader.getOrElse(reader).features(filter)
    new CloseableIterator[ArrowSimpleFeature] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): ArrowSimpleFeature = iter.next()
      override def close(): Unit = CloseWithLogging(Seq(iter) ++ newReader)
    }
  }

  override def close(): Unit = reader.close()
}
