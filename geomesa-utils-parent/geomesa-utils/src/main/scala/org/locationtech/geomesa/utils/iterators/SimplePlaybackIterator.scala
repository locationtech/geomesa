/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureImpl
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.iterators.PlaybackIterator.ff

import java.util.Date

/**
 * Query over a time frame and return the features in sorted order, delayed based on the date of each feature
 * to simulate the original ingestion stream
 *
 * Requires the iterator to be sorted by time
 *
 * @param dtg date attribute to sort by
 * @param filter additional filter predicate, if any
 * @param transforms query transforms, if any
 * @param rate multiplier for the rate of returning features, applied to the original delay between features
 * @param live project dates to current time
 */
class SimplePlaybackIterator(
                        iterator: CloseableIterator[SimpleFeature],
                        sft: SimpleFeatureType,
                        dtg: Option[String] = None,
                        transforms: Array[String] = null,
                        rate: Float = 10f,
                        live: Boolean = false
                      ) extends CloseableIterator[SimpleFeature] with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  //  require(interval._2.after(interval._1), s"Interval is not ordered correctly: ${interval._1}/${interval._2}")

  private val dtgName = dtg.orElse(sft.getDtgField).getOrElse {
    throw new IllegalArgumentException("Schema does not have a default date field")
  }
  private val tdefs = transforms match {
    case null => null
    case t if t.indexOf(dtgName) == -1 => t :+ dtgName
    case t => t
  }
  private val dtgIndex = tdefs match {
    case null => sft.indexOf(dtgName)
    case t => t.indexOf(dtgName)
  }
  require(dtgIndex != -1, "Invalid date field")
  private var start: Long = -1
  private var eventStart: Long = -1

  override def hasNext: Boolean = iterator.hasNext

  override def next(): SimpleFeature = {
    val feature = iterator.next();
    val featureTime = feature.getAttribute(dtgIndex).asInstanceOf[Date].getTime
    if (start == -1L) {
      // emit the first feature as soon as it's available, and set the clock to start timing from here
      start = System.currentTimeMillis()
      logger.info("Starting replay clock at: {}", start)
      eventStart = featureTime
    }
    val featureRelativeTime = start + ((featureTime - eventStart) / rate).toLong
    val sleep = featureRelativeTime - System.currentTimeMillis()
    if (sleep > 0) {
      Thread.sleep(sleep)
    }
    if (live) {
      feature.setAttribute(dtgIndex, new Date(featureRelativeTime))
    }
    feature
  }

  override def close(): Unit = iterator.close()
}
