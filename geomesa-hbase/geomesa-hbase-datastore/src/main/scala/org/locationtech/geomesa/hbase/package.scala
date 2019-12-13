/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object hbase {

  object HBaseSystemProperties {
    val CoprocessorPath = SystemProperty("geomesa.hbase.coprocessor.path")
    val WriteBatchSize = SystemProperty("geomesa.hbase.write.batch")
    val WalDurability = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val ScanBufferSize = SystemProperty("geomesa.hbase.scan.buffer", "100000")
    val TableAvailabilityTimeout = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")
  }
}
