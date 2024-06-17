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
import com.amazonaws.services.s3.model.{ObjectTagging, SetObjectTaggingRequest, Tag}
import org.apache.hadoop.fs.Path

import java.util.Collections

/**
 * Creates a tag containing the base64 encoded summary visibility for the observed file
 *
 * @param path file path
 * @param s3 s3 client
 * @param tag tag name to use
 */
class S3VisibilityObserver(val path: Path, s3: AmazonS3, tag: String) extends AbstractS3VisibilityObserver(path) {
  override protected def makeTagRequest(bucket: String, key: String, visibility: String): Unit = {
    val tagging = new ObjectTagging(Collections.singletonList(new Tag(tag, visibility)))
    val request = new SetObjectTaggingRequest(bucket, key, tagging)
    s3.setObjectTagging(request)
  }
}
