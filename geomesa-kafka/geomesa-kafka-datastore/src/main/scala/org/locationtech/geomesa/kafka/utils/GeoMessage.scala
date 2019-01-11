/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import org.opengis.feature.simple.SimpleFeature

sealed trait GeoMessage

object GeoMessage {

  /**
    * Creates a `Clear` message with the current time
    *
    * @return
    */
  def clear(): Clear = Clear

  /**
    * Creates a `Delete` message with the current time
    *
    * @param id feature id being deleted
    * @return
    */
  def delete(id: String): GeoMessage = Delete(id)

  /**
    * Creates a `Change` message with the current time
    *
    * @param sf simple feature being added/updated
    * @return
    */
  def change(sf: SimpleFeature): GeoMessage = Change(sf)

  /**
    * Message indicating a feature has been added/updated
    *
    * @param feature feature being added/updated
    */
  case class Change(feature: SimpleFeature) extends GeoMessage

  /**
    * Message indicating a feature has been deleted
    *
    * @param id feature id of the feature being deleted
    */
  case class Delete(id: String) extends GeoMessage

  /**
    * Message indicating all features have been deleted
    *
    */
  trait Clear extends GeoMessage

  case object Clear extends Clear
}
