/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa


import java.io.Flushable

import org.geotools.data.simple.SimpleFeatureWriter
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object index {

  type FlushableFeatureWriter = SimpleFeatureWriter with Flushable

  val FilterCacheSize = SystemProperty("geomesa.cache.filters.size", "1000")
  val ZFilterCacheSize = SystemProperty("geomesa.cache.z-filters.size", "1000")

  val PartitionParallelScan = SystemProperty("geomesa.partition.scan.parallel", "false")

  val DistributedLockTimeout = SystemProperty("geomesa.distributed.lock.timeout", "2 minutes")
}
