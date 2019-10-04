/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate

/**
  * Hadoop utilities
  */
object HadoopUtils extends LazyLogging {

  /**
    * Add a resource to the given conf
    *
    * @param conf conf
    * @param path resource path
    */
  def addResource(conf: Configuration, path: String): Unit = {
    // use our path handling logic, which is more robust than just passing paths to the config
    val delegate = if (PathUtils.isRemote(path)) { new HadoopDelegate(conf) } else { PathUtils }
    val handle = delegate.getHandle(path)
    if (!handle.exists) {
      logger.warn(s"Could not load configuration file at: $path")
    } else {
      WithClose(handle.open) { files =>
        files.foreach {
          case (None, is) => conf.addResource(is)
          case (Some(name), is) => conf.addResource(is, name)
        }
        conf.size() // this forces a loading of the resource files, before we close our file handle
      }
    }
  }
}
