/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.data.DataUtilities
import org.geotools.data.store.DataFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
 * Build a unique feature collection based on feature ID
 */
class UniqueMultiCollection(schema: SimpleFeatureType, collections: Iterator[Iterable[SimpleFeature]]) extends DataFeatureCollection {

  private val distinctFeatures = {
    val uniq = collection.mutable.HashMap.empty[String, SimpleFeature]
    collections.flatten.foreach { sf => uniq.put(sf.getID, sf)}
    uniq.values
  }

  override def getBounds: ReferencedEnvelope = DataUtilities.bounds(this)

  override def getCount: Int = openIterator.size
  
  override protected def openIterator = distinctFeatures.iterator

  override def toArray: Array[AnyRef] = openIterator.toArray

  override def getSchema: SimpleFeatureType = schema
}

