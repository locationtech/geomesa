/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object accumulo {

  object AccumuloProperties {

    object AccumuloQueryProperties {
      // if we generate more ranges than this we will split them up into sequential scans
      @deprecated("Use 'geomesa.scan.ranges.target'")
      val SCAN_BATCH_RANGES = SystemProperty("geomesa.scan.ranges.batch", "20000")
    }

    object AccumuloMapperProperties {
      val DESIRED_SPLITS_PER_TSERVER = SystemProperty("geomesa.mapreduce.splits.tserver.max")
      val DESIRED_ABSOLUTE_SPLITS = SystemProperty("geomesa.mapreduce.splits.max")
    }

    object BatchWriterProperties {
      val WRITER_LATENCY      = SystemProperty("geomesa.batchwriter.latency", "60 seconds")
      val WRITER_MEMORY_BYTES = SystemProperty("geomesa.batchwriter.memory", "50mb")
      val WRITER_THREADS      = SystemProperty("geomesa.batchwriter.maxthreads", "10")
      val WRITE_TIMEOUT       = SystemProperty("geomesa.batchwriter.timeout")
    }

    object StatsProperties {
      val STAT_COMPACTION_INTERVAL = SystemProperty("geomesa.stats.compact.interval", "1 hour")
    }
  }
}
