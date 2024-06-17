/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.observer.{FileSystemObserver, FileSystemObserverFactory}

import scala.util.control.NonFatal

/**
 * Factory for S3VisibilityObserver
 */
class S3VisibilityObserverFactory extends FileSystemObserverFactory with LazyLogging {

  private var delegate: FileSystemObserverFactory = _

  override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = {
    try {
      if (S3VisibilityObserverFactory.UseV2) {
        delegate = new v2.S3VisibilityObserverFactory()
      } else {
        delegate = new v1.S3VisibilityObserverFactory()
      }
      delegate.init(conf, root, sft)
    } catch {
      case e: Exception => throw new RuntimeException("Unable to get s3 client", e)
    }
  }

  override def apply(path: Path): FileSystemObserver = delegate.apply(path)

  override def close(): Unit = if (delegate != null) { delegate.close() }
}

object S3VisibilityObserverFactory extends LazyLogging {

  val TagNameConfig = "geomesa.fs.vis.tag"
  val DefaultTag = "geomesa.file.visibility"

  lazy private val UseV2: Boolean = try {
    val versionRegex = """(\d+)\.(\d+)\..*""".r
    val versionRegex(maj, min) = org.apache.hadoop.util.VersionInfo.getVersion
    maj.toInt >= 3 && min.toInt >= 4
  } catch {
    case NonFatal(e) => logger.warn("Unable to evaluate hadoop version, defaulting to aws sdk v2: ", e); true
  }
}
