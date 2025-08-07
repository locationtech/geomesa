/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}

object StorageJobUtils extends LazyLogging {

  @deprecated("Replaced with org.locationtech.geomesa.tools.utils.DistributedCopy")
  def distCopy(
      srcRoot: Path,
      destRoot: Path,
      statusCallback: StatusCallback,
      conf: Configuration = new Configuration()): JobResult =
    new DistributedCopy(conf).copy(srcRoot, destRoot, statusCallback)
}
