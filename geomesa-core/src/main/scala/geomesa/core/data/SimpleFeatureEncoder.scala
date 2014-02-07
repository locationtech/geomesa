/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import geomesa.utils.text.ObjectPoolFactory
import org.apache.accumulo.core.data.Value
import org.geotools.data.DataUtilities
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}


/**
 * Responsible for collecting data-entries that share an identifier, and
 * when done, collapsing these rows into a single SimpleFeature.
 */

object SimpleFeatureEncoder {
  def encode(feature:SimpleFeature) : Value =
    new Value(ThreadSafeDataUtilities.encodeFeature(feature).getBytes)

  def decode(simpleFeatureType: SimpleFeatureType, featureValue: Value) = {
    ThreadSafeDataUtilities.createFeature(simpleFeatureType, featureValue.toString)
  }
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature) : String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String) : SimpleFeature =
    dataUtilitiesPool.withResource {
    _ => DataUtilities.createFeature(simpleFeatureType, featureString)
  }
}
