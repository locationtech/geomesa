/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3
package v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.utils.io.CloseQuietly
import software.amazon.awssdk.services.s3.S3Client

import java.io.IOException

/**
 * Visibility observer for aws sdk v2
 */
class S3VisibilityObserverFactory extends FileSystemObserverFactory {

  private var fs: S3AFileSystem = _
  private var s3: S3Client = _
  private var tag: String = _

  override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = {
    try {
      fs = root.getFileSystem(conf).asInstanceOf[S3AFileSystem]
      s3 = fs.getS3AInternals.getAmazonS3Client("tags")
      tag = conf.get(S3VisibilityObserverFactory.TagNameConfig, S3VisibilityObserverFactory.DefaultTag)
    } catch {
      case e: Exception => throw new RuntimeException("Unable to get s3 client", e)
    }
  }

  override def apply(path: Path): FileSystemObserver = new S3VisibilityObserver(path, s3, tag)

  override def close(): Unit = {
    if (fs != null) {
      val err = CloseQuietly(fs)
      s3 = null
      fs = null
      err.foreach(e => throw new IOException("Error closing S3 filesystem", e))
    }
  }
}
