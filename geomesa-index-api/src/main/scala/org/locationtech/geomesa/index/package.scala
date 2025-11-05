/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa


import org.geotools.api.data.SimpleFeatureWriter
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import java.io.Flushable

package object index {

  type FlushableFeatureWriter = SimpleFeatureWriter with Flushable

  val FilterCacheSize : SystemProperty = SystemProperty("geomesa.cache.filters.size", "1000")
  val ZFilterCacheSize: SystemProperty = SystemProperty("geomesa.cache.z-filters.size", "1000")

  val PartitionParallelScan : SystemProperty = SystemProperty("geomesa.partition.scan.parallel", "false")
  val DistributedLockTimeout: SystemProperty = SystemProperty("geomesa.distributed.lock.timeout", "2 minutes")
}
