/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
