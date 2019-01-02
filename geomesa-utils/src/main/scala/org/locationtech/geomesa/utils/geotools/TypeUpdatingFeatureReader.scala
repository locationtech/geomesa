/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.data.{DelegatingFeatureReader, FeatureReader, ReTypeFeatureReader}
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

  override def next(): SimpleFeature = FeatureUtils.retype(delegate.next(), featureType)

  override def hasNext: Boolean = delegate.hasNext
  override def getFeatureType: SimpleFeatureType = featureType
  override def close(): Unit = delegate.close()
}