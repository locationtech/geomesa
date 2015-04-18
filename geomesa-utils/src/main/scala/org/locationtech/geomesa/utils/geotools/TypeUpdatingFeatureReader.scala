/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.utils.geotools

import org.geotools.data.{DataUtilities, DelegatingFeatureReader, FeatureReader, ReTypeFeatureReader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/** A [[DelegatingFeatureReader]] that re-types simple features.  Unlike [[ReTypeFeatureReader]] this
  * feature reader will preserve user data.
  *
  * @param delegate the delegate reader
  * @param featureType the projected type
  */
class TypeUpdatingFeatureReader(delegate: FeatureReader[SimpleFeatureType, SimpleFeature],
                                featureType: SimpleFeatureType)
  extends DelegatingFeatureReader[SimpleFeatureType, SimpleFeature] {

  override val getDelegate: FeatureReader[SimpleFeatureType, SimpleFeature] = delegate

  override def next(): SimpleFeature = {
    val org = delegate.next()
    val mod = DataUtilities.reType(featureType, org, false)
    mod.getUserData.putAll(org.getUserData)
    mod
  }

  override def hasNext: Boolean = delegate.hasNext
  override def getFeatureType: SimpleFeatureType = featureType
  override def close(): Unit = delegate.close()
}