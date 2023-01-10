/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

import org.locationtech.geomesa.kafka.streams.MessageAction.MessageAction

/**
 * Data model for a GeoMesa data store message, used as the value in a Kafka record
 *
 * @param action message action
 * @param attributes attributes of the simple feature represented by this message
 * @param userData user data of the simple feature represented by this message
 */
case class GeoMesaMessage(action: MessageAction, attributes: Seq[AnyRef], userData: Map[String, String] = Map.empty) {

  import scala.collection.JavaConverters._

  def asJava(): java.util.List[AnyRef] = attributes.asJava
}

object GeoMesaMessage {

  import scala.collection.JavaConverters._

  /**
   * Create an upsert message
   *
   * @param attributes feature attribute values
   * @return
   */
  def upsert(attributes: Seq[AnyRef]): GeoMesaMessage = GeoMesaMessage(MessageAction.Upsert, attributes)

  /**
   * Create an upsert message
   *
   * @param attributes feature attribute values
   * @return
   */
  def upsert(attributes: java.util.List[AnyRef]): GeoMesaMessage = upsert(attributes.asScala.toSeq)

  /**
   * Create an upsert message
   *
   * @param attributes feature attribute values
   * @param userData feature user data
   * @return
   */
  def upsert(attributes: Seq[AnyRef], userData: Map[String, String]): GeoMesaMessage =
    GeoMesaMessage(MessageAction.Upsert, attributes, userData)

  /**
   * Create an upsert message
   *
   * @param attributes feature attribute values
   * @return
   */
  def upsert(attributes: java.util.List[AnyRef], userData: java.util.Map[String, String]): GeoMesaMessage =
    upsert(attributes.asScala.toSeq, userData.asScala.toMap)

  /**
   * Create a delete message
   *
   * @return
   */
  def delete(): GeoMesaMessage = GeoMesaMessage(MessageAction.Delete, Seq.empty)
}
