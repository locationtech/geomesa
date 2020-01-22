/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import java.io.IOException

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectTagging, SetObjectTaggingRequest, Tag}
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver

/**
 * Abstract baseclass for writing s3 object tags
 *
 * @param s3 s3 client
 * @param path file path
 */
abstract class S3ObjectTagObserver(s3: AmazonS3, path: Path) extends FileSystemObserver {

  private val (bucket, key) = {
    val uri = path.toUri
    val uriPath = uri.getPath
    val key = if (uriPath.startsWith("/")) { uriPath.substring(1) } else { uriPath }
    (uri.getHost, key)
  }

  /**
   * Return the tags to set on this file
   *
   * @return
   */
  protected def tags(): Iterable[(String, String)]

  override def flush(): Unit = {}

  override def close(): Unit = {
    val iter = tags()
    if (iter.nonEmpty) {
      try {
        val list = new java.util.ArrayList[Tag]()
        iter.foreach { case (k, v) => list.add(new Tag(k, v)) }
        s3.setObjectTagging(new SetObjectTaggingRequest(bucket, key, new ObjectTagging(list)))
      } catch {
        case e: Exception => throw new IOException("Error tagging object", e)
      }
    }
  }
}
