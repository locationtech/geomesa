/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
