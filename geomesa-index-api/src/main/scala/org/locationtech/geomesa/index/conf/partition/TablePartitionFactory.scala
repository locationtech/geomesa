/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.partition

import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.opengis.feature.simple.SimpleFeatureType

trait TablePartitionFactory {

  /**
    * Name used to identify this partitioning scheme, used for SPI loading
    *
    * @return
    */
  def name: String

  /**
    * Create a partitioning scheme
    *
    * @param sft simple feature type
    * @return
    */
  def create[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](ds: GeoMesaDataStore[DS, F, W], sft: SimpleFeatureType): TablePartition
}
