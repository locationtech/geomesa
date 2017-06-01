/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.locationtech.geomesa.utils.geotools.FR
import org.opengis.filter.Filter

trait LiveFeatureCache {
  def cleanUp(): Unit
  def createOrUpdateFeature(update: CreateOrUpdate): Unit
  def removeFeature(toDelete: Delete): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def getFeatureById(id: String): FeatureHolder
  def getReaderForFilter(filter: Filter): FR
}