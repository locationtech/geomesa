/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.versions

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Reflection wrapper for ProducerRecord and ConsumerRecord methods between kafka versions
  * 0.9, 0.10, 1.0, 1.1, and 2.0
  */
// noinspection LanguageFeature
object RecordVersions extends LazyLogging {

  private val producerMethods = classOf[ProducerRecord[_, _]].getDeclaredMethods
  private val consumerMethods = classOf[ConsumerRecord[_, _]].getDeclaredMethods

  def setHeader(record: ProducerRecord[_, _], key: String, value: Array[Byte]): Unit = _setHeader(record, key, value)

  def getHeaders(record: ConsumerRecord[_, _]): Map[String, Array[Byte]] = _getHeaders(record)

  def getTimestamp(record:ConsumerRecord[_,_]): Long = _getTimestamp(record)

  private val _setHeader: (ProducerRecord[_, _], String, Array[Byte]) => Unit = {
    producerMethods.find(m => m.getName == "headers" && m.getParameterCount == 0) match {
      case Some(method) => (record, k, v) => method.invoke(record).asInstanceOf[Headers].add(k, v)
      case None =>
        logger.warn("This version of Kafka doesn't support message headers, serialization may be slower")
        (_, _, _) => ()
    }
  }

  private val _getHeaders: ConsumerRecord[_, _] => Map[String, Array[Byte]] = {
    consumerMethods.find(m => m.getName == "headers" && m.getParameterCount == 0) match {
      case Some(method) =>
        record => {
          val headers = method.invoke(record).asInstanceOf[Headers].iterator()
          val builder = Map.newBuilder[String, Array[Byte]]
          while (headers.hasNext) {
            val header = headers.next()
            builder += header.key -> header.value
          }
          builder.result()
        }

      case None =>
        logger.warn("This version of Kafka doesn't support message headers, serialization may be slower")
        _ => Map.empty
    }
  }

  private val _getTimestamp: ConsumerRecord[_, _] => Long = {
    consumerMethods.find(m => m.getName == "timestamp" && m.getParameterCount == 0).map{ method =>
      record: ConsumerRecord[_,_] => method.invoke(record).asInstanceOf[Long]
    }.getOrElse{
      logger.warn("This version of Kafka doesn't support message timestamps, confluent serialization not supported")
      _: ConsumerRecord[_,_] => System.currentTimeMillis
    }
  }

  private type Headers = java.lang.Iterable[Header] {
    def add(key: String, value: Array[Byte]): AnyRef
  }

  private type Header = AnyRef {
    def key(): String
    def value(): Array[Byte]
  }
}
