/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.transform

import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializers}
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
