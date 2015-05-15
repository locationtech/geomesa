/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.util

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

