/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.amazonaws.services.s3.AmazonS3
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.security.VisibilityEvaluator.VisibilityAnd
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.opengis.feature.simple.SimpleFeature

/**
 * Creates a tag containing the base64 encoded summary visibility for the observed file
 *
 * @param s3 s3 client
 * @param path file path
 * @param tag tag name to use
 */
class S3VisibilityObserver(s3: AmazonS3, path: Path, tag: String) extends S3ObjectTagObserver(s3, path) {

  private val visibilities = scala.collection.mutable.Set.empty[String]

  override def write(feature: SimpleFeature): Unit = {
    val vis = SecurityUtils.getVisibility(feature)
    if (vis != null) {
      visibilities.add(vis)
    }
  }

  override protected def tags(): Iterable[(String, String)] = {
    if (visibilities.isEmpty) { Seq.empty } else {
      val expressions = visibilities.flatMap { v =>
        VisibilityEvaluator.parse(v) match {
          case VisibilityAnd(clauses) => clauses
          case e => Seq(e)
        }
      }
      val vis = VisibilityAnd(expressions.toSeq).expression
      Seq(tag -> Base64.getEncoder.encodeToString(vis.getBytes(StandardCharsets.UTF_8)))
    }
  }
}
