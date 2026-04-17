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

import org.apache.accumulo.access.AccessExpression
import org.geotools.api.feature.simple.SimpleFeature
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

  private val (bucket, key) = {
    val uriPath = path.getPath
    val key = if (uriPath.startsWith("/")) { uriPath.substring(1) } else { uriPath }
    (path.getHost, key)
  }

  override def apply(feature: SimpleFeature): Unit = {
    val vis = SecurityUtils.getVisibility(feature)
    if (vis != null) {
      visibilities.add(vis)
    }
  }

  override def flush(): Unit = {}

  override def close(): Unit = {
    try { makeTagRequest(bucket, key) } catch {
      case e: Exception => throw new IOException("Error tagging object", e)
    }
  }

  private def makeTagRequest(bucket: String, key: String): Unit = {
    if (visibilities.nonEmpty) {
      val vis = visibilities.mkString("(", ")&(", ")")
      // this call simplifies and de-duplicates the expression
      val expression = AccessExpression.of(vis, /*normalize = */true).getExpression
      val visibility = Base64.getEncoder.encodeToString(expression.getBytes(StandardCharsets.UTF_8))
      val tagging = Tagging.builder().tagSet(Tag.builder.key(tag).value(visibility).build()).build()
      val request = PutObjectTaggingRequest.builder.bucket(bucket).key(key).tagging(tagging).build()
      s3.putObjectTagging(request).join()
    }
  }
}
