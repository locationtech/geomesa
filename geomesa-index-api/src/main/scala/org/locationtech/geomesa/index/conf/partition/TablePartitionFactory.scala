/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.partition

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.metadata.HasGeoMesaMetadata

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
  def create(ds: HasGeoMesaMetadata[String], sft: SimpleFeatureType): TablePartition
}
