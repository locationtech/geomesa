/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class HBaseFeature(val feature: SimpleFeature,
                   serializer: SimpleFeatureSerializer,
                   defaultVisibility: Option[String] = None) extends WrappedFeature {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}
  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

  lazy val fullValue = new RowValue(DataColumnFamily, DataColumnQualifier, visibility, serializer.serialize(feature))

  private lazy val visibility =
    feature.userData[String](FEATURE_VISIBILITY).orElse(defaultVisibility).map(new CellVisibility(_))
}

class RowValue(val cf: Array[Byte], val cq: Array[Byte], val vis: Option[CellVisibility], toValue: => Array[Byte]) {
  lazy val value: Array[Byte] = toValue
}
