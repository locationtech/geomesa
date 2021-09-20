/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> 735150f8a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0696f5a4bc (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eae3ebc078 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 51c0ba8e46 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9856b9da03 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f8d2a0595c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 735150f8a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0696f5a4bc (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eae3ebc078 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 51c0ba8e46 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9856b9da03 (GEOMESA-3100 Kafka layer views (#2784))
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
