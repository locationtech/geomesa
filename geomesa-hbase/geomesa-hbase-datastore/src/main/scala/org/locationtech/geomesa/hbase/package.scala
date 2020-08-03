/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object hbase {

  object HBaseSystemProperties {
    val CoprocessorUrl           : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.url")
    val CoprocessorMaxThreads    : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.maximize.threads", "true")
    val WriteBatchSize           : SystemProperty = SystemProperty("geomesa.hbase.write.batch")
    val WalDurability            : SystemProperty = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching           : SystemProperty = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching      : SystemProperty = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val ScanBufferSize           : SystemProperty = SystemProperty("geomesa.hbase.scan.buffer", "100000")
    val TableAvailabilityTimeout : SystemProperty = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")

    @deprecated("Use coprocessor url")
    val CoprocessorPath: SystemProperty = SystemProperty("geomesa.hbase.coprocessor.path")
  }
}
