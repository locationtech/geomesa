/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.locationtech.geomesa.accumulo.AccumuloProperties

object GeoMesaBatchWriterConfig extends LazyLogging {

  def apply(threads: Option[Int] = None,
            memory: Option[Long] = None,
            latency: Option[Long] = None,
            timeout: Option[Long] = None): BatchWriterConfig = {
    import AccumuloProperties.BatchWriterProperties

    val bwc = new BatchWriterConfig

    threads.orElse(BatchWriterProperties.WRITER_THREADS.option.map(_.toInt)).foreach { threads =>
      logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads")
      bwc.setMaxWriteThreads(threads)
    }

    memory.orElse(BatchWriterProperties.WRITER_MEMORY_BYTES.toBytes).foreach { memory =>
      logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes")
      bwc.setMaxMemory(memory)
    }

    latency.orElse(BatchWriterProperties.WRITER_LATENCY.toDuration.map(_.toMillis)).foreach { latency =>
      logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency millis")
      bwc.setMaxLatency(latency, TimeUnit.MILLISECONDS)
    }

    timeout.orElse(BatchWriterProperties.WRITE_TIMEOUT.toDuration.map(_.toMillis)).foreach { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout millis")
      bwc.setTimeout(timeout, TimeUnit.MILLISECONDS)
    }

    bwc
  }
}
