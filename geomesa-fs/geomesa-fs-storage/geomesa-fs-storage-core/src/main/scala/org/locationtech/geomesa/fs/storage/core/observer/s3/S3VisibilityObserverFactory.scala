/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package observer
package s3

import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.net.URI

/**
 * Visibility observer for aws sdk v2
 */
class S3VisibilityObserverFactory extends FileSystemObserverFactory {

  private var fs: S3ObjectStore = _
  private var s3: S3AsyncClient = _
  private var tag: String = _

  override def init(storage: FileSystemStorage): Unit = {
    try {
      fs = storage.fs.asInstanceOf[S3ObjectStore]
      s3 = fs.client
      tag = storage.context.conf.getOrElse(S3VisibilityObserverFactory.TagNameConfig, S3VisibilityObserverFactory.DefaultTag)
    } catch {
      case e: Exception => throw new RuntimeException("Unable to get s3 client", e)
    }
  }

  override def apply(path: URI): FileSystemObserver = new S3VisibilityObserver(path, s3, tag)

  override def close(): Unit = {}
}

object S3VisibilityObserverFactory {

  val TagNameConfig = "geomesa.fs.vis.tag"
  val DefaultTag = "geomesa.file.visibility"
}
