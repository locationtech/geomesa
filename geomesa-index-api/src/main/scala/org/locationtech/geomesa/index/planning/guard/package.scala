/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import org.locationtech.geomesa.filter.Bounds

import scala.concurrent.duration.{Duration, FiniteDuration}

package object guard {
  protected [planning] def duration(values: Seq[Bounds[ZonedDateTime]]): FiniteDuration = {
    values.foldLeft(Duration.Zero) { (sum, bounds) =>
      sum + Duration(bounds.upper.value.get.toEpochSecond - bounds.lower.value.get.toEpochSecond, TimeUnit.SECONDS)
    }
  }
}
