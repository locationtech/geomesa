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

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.security.SecurityUtils
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{PutObjectTaggingRequest, Tag, Tagging}

import java.io.IOException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * Creates a tag containing the base64 encoded summary visibility for the observed file
 *
 * @param path file path
 * @param s3 s3 client
 * @param tag tag name to use
 */
class S3VisibilityObserver(path: URI, s3: S3AsyncClient, tag: String) extends FileSystemObserver {

  private val visibilities = scala.collection.mutable.Set.empty[String]

  private val key = S3ObjectStore.parseS3Path(path)

  override def apply(feature: SimpleFeature): Unit = {
    val vis = SecurityUtils.getVisibility(feature)
    if (vis != null) {
      visibilities.add(vis)
    }
  }

  override def flush(): Unit = {}

  override def close(): Unit = {
    try { makeTagRequest(key.bucket, key.key) } catch {
      case e: Exception => throw new IOException("Error tagging object", e)
    }
  }

  private def makeTagRequest(bucket: String, key: String): Unit = {
    if (visibilities.nonEmpty) {
      val vis = visibilities.toList.sorted.mkString("(", ")&(", ")")
      // TODO simplify and de-duplicates the expression
      val visibility = Base64.getEncoder.encodeToString(vis.getBytes(StandardCharsets.UTF_8))
      val tagging = Tagging.builder().tagSet(Tag.builder.key(tag).value(visibility).build()).build()
      val request = PutObjectTaggingRequest.builder.bucket(bucket).key(key).tagging(tagging).build()
      s3.putObjectTagging(request).join()
    }
  }
}
