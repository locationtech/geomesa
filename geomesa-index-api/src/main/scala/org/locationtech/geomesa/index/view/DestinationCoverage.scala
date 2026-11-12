/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.io.Closeable
import java.time.Instant

/** A producer-certified, contiguous, queryable and final event-time interval [from, through). */
case class CoverageInterval(from: Instant, through: Instant) {
  require(from != null && through != null && from.isBefore(through), "Invalid destination coverage interval")
  require(Seq(from, through).forall(i => Instant.ofEpochMilli(i.toEpochMilli) == i),
    "Destination coverage boundaries must have millisecond precision")
}

/**
 * Dataset IDs are feature type names, scoped to the configured destination.
 * Publication and finality are the provider's responsibility; this interface does not prescribe a producer.
 */
trait DestinationCoverage extends Closeable {
  def coverage(datasetId: String): Option[CoverageInterval]
}
