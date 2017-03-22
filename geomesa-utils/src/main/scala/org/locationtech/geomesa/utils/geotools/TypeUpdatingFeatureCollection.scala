/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util

import org.geotools.data.DataUtilities
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.factory.Hints
import org.geotools.feature.collection.AbstractFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class TypeUpdatingFeatureCollection(collection: SimpleFeatureCollection, newType: SimpleFeatureType) extends AbstractFeatureCollection(newType) {

  val delegate = new TypeUpdatingFeatureIterator(collection.features, newType)

  def openIterator(): util.Iterator[SimpleFeature] = delegate

  def closeIterator(close: util.Iterator[SimpleFeature]) {delegate.remove()}

  def size(): Int = collection.size

  def getBounds: ReferencedEnvelope = collection.getBounds
}

class TypeUpdatingFeatureIterator(delegate: SimpleFeatureIterator, newType: SimpleFeatureType) extends util.Iterator[SimpleFeature] {
  def hasNext: Boolean = delegate.hasNext

  def next(): SimpleFeature = {
    val delegateNext = delegate.next
    val newFeature = DataUtilities.reType(newType, delegateNext)
    newFeature.setDefaultGeometry(delegateNext.getDefaultGeometry)
    newFeature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(x = true))
    newFeature
  }

  def remove() {delegate.close()}
}