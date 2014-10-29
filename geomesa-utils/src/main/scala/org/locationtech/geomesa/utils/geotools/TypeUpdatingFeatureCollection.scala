/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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