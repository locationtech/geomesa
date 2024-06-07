/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import org.apache.accumulo.access.AccessExpression
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.security.SecurityUtils

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Base64

abstract class AbstractS3VisibilityObserver(path: Path) extends FileSystemObserver {

  private val visibilities = scala.collection.mutable.Set.empty[String]

  private val (bucket, key) = {
    val uri = path.toUri
    val uriPath = uri.getPath
    val key = if (uriPath.startsWith("/")) { uriPath.substring(1) } else { uriPath }
    (uri.getHost, key)
  }

  override def flush(): Unit = {}

  override def close(): Unit = {
    try { makeTagRequest(bucket, key) } catch {
      case e: Exception => throw new IOException("Error tagging object", e)
    }
  }
  override def write(feature: SimpleFeature): Unit = {
    val vis = SecurityUtils.getVisibility(feature)
    if (vis != null) {
      visibilities.add(vis)
    }
  }

  private def makeTagRequest(bucket: String, key: String): Unit = {
    if (visibilities.nonEmpty) {
      val vis = visibilities.mkString("(", ")&(", ")")
      // this call simplifies and de-duplicates the expression
      val expression = AccessExpression.of(vis, /*normalize = */true).getExpression
      makeTagRequest(bucket: String, key: String, Base64.getEncoder.encodeToString(expression.getBytes(StandardCharsets.UTF_8)))
    }
  }

  protected def makeTagRequest(bucket: String, key: String, visibility: String): Unit
}
