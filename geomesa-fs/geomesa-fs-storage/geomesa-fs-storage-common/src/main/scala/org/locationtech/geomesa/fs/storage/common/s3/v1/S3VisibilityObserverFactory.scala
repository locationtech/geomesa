/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3
package v1

import com.amazonaws.services.s3.AmazonS3
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.io.IOException

/**
 * Visibility observer for aws sdk v1
 */
class S3VisibilityObserverFactory extends FileSystemObserverFactory {

  private var fs: FileSystem = _
  private var s3: AmazonS3 = _
  private var tag: String = _

  override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = {
    try {
      // use reflection to access to private client factory used by the s3a hadoop impl
      fs = root.getFileSystem(conf)
      val field = fs.getClass.getDeclaredField("s3")
      field.setAccessible(true)
      s3 = field.get(fs).asInstanceOf[AmazonS3]
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
