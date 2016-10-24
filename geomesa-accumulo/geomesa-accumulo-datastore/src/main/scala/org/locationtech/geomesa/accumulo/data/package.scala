/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.data.Value
import org.apache.hadoop.io.Text
import org.geotools.data.FeatureWriter
import org.geotools.factory.Hints.ClassKey
import org.locationtech.geomesa.features.SerializationType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

package object data {

  // Storage implementation constants
  val EMPTY_STRING         = ""
  val EMPTY_VALUE          = new Value(Array[Byte]())
  val EMPTY_COLF           = new Text(EMPTY_STRING)
  val EMPTY_COLQ           = new Text(EMPTY_STRING)
  val EMPTY_VIZ            = new Text(EMPTY_STRING)
  val EMPTY_TEXT           = new Text()
  val DEFAULT_ENCODING     = SerializationType.KRYO

  // SimpleFeature Hints
  val TRANSFORMS           = new ClassKey(classOf[String])
  val TRANSFORM_SCHEMA     = new ClassKey(classOf[SimpleFeatureType])

  type SFFeatureWriter = FeatureWriter[SimpleFeatureType, SimpleFeature]
}
