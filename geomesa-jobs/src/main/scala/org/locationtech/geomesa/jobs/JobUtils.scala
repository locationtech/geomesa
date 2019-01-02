/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

object JobUtils extends LazyLogging {

  /**
   * Sets the libjars into a Hadoop configuration. Will search the environment first, then the
   * classpath, until all required jars have been found.
   *
   * @param conf job configuration
   * @param libJars jar prefixes to load
   */
  def setLibJars(conf: Configuration, libJars: Seq[String], searchPath: Iterator[() => Seq[File]]): Unit = {
    val extra = ClassPathUtils.loadClassPathFromEnv("GEOMESA_EXTRA_CLASSPATHS")
    val found = ClassPathUtils.findJars(libJars, searchPath)
    // always prepend GEOMESA_EXTRA_CLASSPATHS first
    val paths = (extra ++ found).map(f => "file://" + f.getAbsolutePath)
    // tmpjars is the hadoop config that corresponds to libjars
    if (paths.nonEmpty) {
      conf.setStrings("tmpjars", paths: _*)
    }
    logger.debug(s"Job will use the following libjars:${paths.mkString("\n\t", "\n\t", "")}")
  }
}
