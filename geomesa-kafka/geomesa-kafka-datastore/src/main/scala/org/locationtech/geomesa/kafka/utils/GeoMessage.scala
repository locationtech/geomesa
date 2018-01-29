/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.time.Instant

import org.opengis.feature.simple.SimpleFeature

sealed trait GeoMessage {
  def timestamp: Instant
}

object GeoMessage {

  /**
    * Creates a `Clear` message with the current time
    *
    * @return
    */
  def clear(): Clear = Clear(Instant.now)

  /**
    * Creates a `Delete` message with the current time
    *
    * @param id feature id being deleted
    * @return
    */
  def delete(id: String): Delete = Delete(Instant.now, id)

  /**
    * Creates a `Change` message with the current time
    *
    * @param sf simple feature being added/updated
    * @return
    */
  def change(sf: SimpleFeature): Change = Change(Instant.now, sf)

  /**
    * Message indicating a feature has been added/updated
    *
    * @param timestamp time of the message
    * @param feature feature being added/updated
    */
  case class Change(override val timestamp: Instant, feature: SimpleFeature) extends GeoMessage

  /**
    * Message indicating a feature has been deleted
    *
    * @param timestamp time of the message
    * @param id feature id of the feature being deleted
    */
  case class Delete(override val timestamp: Instant, id: String) extends GeoMessage

  /**
    * Message indicating all features have been deleted
    *
    * @param timestamp time of the message
    */
  case class Clear(override val timestamp: Instant) extends GeoMessage

}
