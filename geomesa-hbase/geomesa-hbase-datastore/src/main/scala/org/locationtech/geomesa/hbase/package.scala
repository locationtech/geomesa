/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object hbase {

  object HBaseSystemProperties {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
>>>>>>> 2ee6a1e51f (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
    val CoprocessorUrl             : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.url")
    val CoprocessorMaxThreads      : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.maximize.threads", "true")
    val WriteBatchSize             : SystemProperty = SystemProperty("geomesa.hbase.write.batch")
    val WriteFlushTimeout          : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
    val WalDurability              : SystemProperty = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching             : SystemProperty = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching        : SystemProperty = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val ScanBufferSize             : SystemProperty = SystemProperty("geomesa.hbase.scan.buffer", "100000")
    val TableAvailabilityTimeout   : SystemProperty = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")
    val DeleteVis                  : SystemProperty = SystemProperty("geomesa.hbase.delete.vis")
    val ConfigPathProperty         : SystemProperty = SystemProperty("geomesa.hbase.config.paths")
    val RemoteFilterProperty       : SystemProperty = SystemProperty("geomesa.hbase.remote.filtering", "true")
    val RemoteArrowProperty        : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.arrow.enable")
    val RemoteBinProperty          : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.bin.enable")
    val RemoteDensityProperty      : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.density.enable")
    val RemoteStatsProperty        : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.stats.enable")
    val YieldPartialResultsProperty: SystemProperty = SystemProperty("geomesa.hbase.coprocessor.yield.partial.results")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ee6a1e51f (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
    val CoprocessorUrl           : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.url")
    val CoprocessorMaxThreads    : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.maximize.threads", "true")
    val WriteBatchSize           : SystemProperty = SystemProperty("geomesa.hbase.write.batch")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
    val WalDurability            : SystemProperty = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching           : SystemProperty = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching      : SystemProperty = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val ScanBufferSize           : SystemProperty = SystemProperty("geomesa.hbase.scan.buffer", "100000")
    val TableAvailabilityTimeout : SystemProperty = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")
    val DeleteVis                : SystemProperty = SystemProperty("geomesa.hbase.delete.vis")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
>>>>>>> locatelli-main
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ee6a1e51f (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e002b864a (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locatelli-main

    @deprecated("Use coprocessor url")
    val CoprocessorPath: SystemProperty = SystemProperty("geomesa.hbase.coprocessor.path")
  }
}
