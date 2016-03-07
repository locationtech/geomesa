/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamo.core

import com.vividsolutions.jts.geom.Point
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait SchemaValidation {

  protected def validatingCreateSchema(featureType: SimpleFeatureType, cs: (SimpleFeatureType) => Unit): Unit = {
    // validate dtg
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[java.util.Date]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a dtg field"))

    // validate geometry
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[Point]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a valid point geometry"))

    cs(featureType)
  }

}
