/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    val CoprocessorThreadsProperty : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.threads", "16")
    val MaxRangesPerExtendedScanProperty   : SystemProperty = SystemProperty("geomesa.hbase.ranges.max-per-extended-scan", "100")
    val MaxRangesPerCoprocessorScanProperty: SystemProperty = SystemProperty("geomesa.hbase.ranges.max-per-coprocessor-scan", Int.MaxValue.toString)
=======
=======
>>>>>>> 202ecaea0e (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> ca6fe239f6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6fb402b89d (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> d082fbb12a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 37d4188d10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc1e06182b (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> f291de6965 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 523f31ca47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ee6a1e51f (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val CoprocessorUrl           : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.url")
    val CoprocessorMaxThreads    : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.maximize.threads", "true")
    val WriteBatchSize           : SystemProperty = SystemProperty("geomesa.hbase.write.batch")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
    val WriteFlushTimeout        : SystemProperty = SystemProperty("geomesa.hbase.write.flush.timeout.millis")
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
    val WalDurability            : SystemProperty = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching           : SystemProperty = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching      : SystemProperty = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val ScanBufferSize           : SystemProperty = SystemProperty("geomesa.hbase.scan.buffer", "100000")
    val TableAvailabilityTimeout : SystemProperty = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")
    val DeleteVis                : SystemProperty = SystemProperty("geomesa.hbase.delete.vis")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9312bc69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 202ecaea0e (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 6fb402b89d (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> dc1e06182b (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 523f31ca47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca6fe239f6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 53f64a9fef (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
>>>>>>> 6fb402b89d (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d082fbb12a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 37d4188d10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2ee6a1e51f (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
<<<<<<< HEAD
>>>>>>> dc1e06182b (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f291de6965 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))

    @deprecated("Use coprocessor url")
    val CoprocessorPath: SystemProperty = SystemProperty("geomesa.hbase.coprocessor.path")
  }
}
