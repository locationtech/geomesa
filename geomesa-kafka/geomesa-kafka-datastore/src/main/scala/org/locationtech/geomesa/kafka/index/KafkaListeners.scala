/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.typesafe.scalalogging.StrictLogging
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent
import org.opengis.feature.simple.SimpleFeature

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

trait KafkaListeners extends StrictLogging {

  import scala.collection.JavaConverters._

  // use a flag instead of checking listeners.isEmpty, which is slightly expensive for ConcurrentHashMap
  @volatile
  private var hasListeners = false

  private val listeners = {
    val map = new ConcurrentHashMap[(SimpleFeatureSource, FeatureListener), java.lang.Boolean]()
    Collections.newSetFromMap(map).asScala
  }

  def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = synchronized {
    listeners.add((source, listener))
    hasListeners = true
  }

  def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = synchronized {
    listeners.remove((source, listener))
    hasListeners = listeners.nonEmpty
  }

  private[kafka] def fireChange(timestamp: Long, feature: SimpleFeature): Unit = {
    if (hasListeners) {
      fireEvent(KafkaFeatureEvent.changed(_, feature, timestamp))
    }
  }

  private[kafka] def fireDelete(timestamp: Long, id: String, removed: => SimpleFeature): Unit = {
    if (hasListeners) {
      fireEvent(KafkaFeatureEvent.removed(_, id, removed, timestamp))
    }
  }

  private[kafka] def fireClear(timestamp: Long): Unit = {
    if (hasListeners) {
      fireEvent(KafkaFeatureEvent.cleared(_, timestamp))
    }
  }

  private def fireEvent(toEvent: SimpleFeatureSource => FeatureEvent): Unit = {
    listeners.foreach { case (source, listener) =>
      val event = toEvent(source)
      try { listener.changed(event) } catch {
        case NonFatal(e) => logger.error(s"Error in feature listener for $event", e)
      }
    }
  }
}
