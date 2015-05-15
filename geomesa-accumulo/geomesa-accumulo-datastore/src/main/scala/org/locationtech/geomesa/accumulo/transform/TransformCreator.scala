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

package org.locationtech.geomesa.accumulo.transform

import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SimpleFeatureSerializers, ScalaSimpleFeature, SimpleFeatureSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object TransformCreator {

  /**
   * Create a function to transform a feature from one sft to another...this will
   * result in a new feature instance being created and encoded.
   *
   * The function returned may NOT be ThreadSafe to due the fact it contains a
   * SimpleFeatureEncoder instance which is not thread safe to optimize performance
   */
  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoding: SerializationType,
                      transformString: String): (SimpleFeature => Array[Byte]) = {

    val encoder = SimpleFeatureSerializers(targetFeatureType, featureEncoding)
    val defs = TransformProcess.toDefinition(transformString)

    val newSf = new ScalaSimpleFeature("reusable", targetFeatureType)

    (feature: SimpleFeature) => {
        newSf.getIdentifier.setID(feature.getIdentifier.getID)
        defs.foreach { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
        encoder.serialize(newSf)
      }
  }
}
