/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class HBaseFeature(val feature: SimpleFeature, serializer: SimpleFeatureSerializer) extends WrappedFeature {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  lazy val fullValue = new RowValue(DataColumnFamily, DataColumnQualifier, serializer.serialize(feature))
}

class RowValue(val cf: Array[Byte], val cq: Array[Byte], toValue: => Array[Byte]) {
  lazy val value: Array[Byte] = toValue
}
