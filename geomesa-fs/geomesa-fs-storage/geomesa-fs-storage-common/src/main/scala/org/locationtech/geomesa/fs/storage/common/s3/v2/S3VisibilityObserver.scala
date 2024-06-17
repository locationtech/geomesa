/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3
package v2

import org.apache.hadoop.fs.Path
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectTaggingRequest, Tag, Tagging}

/**
 * Creates a tag containing the base64 encoded summary visibility for the observed file
 *
 * @param path file path
 * @param s3 s3 client
 * @param tag tag name to use
 */
class S3VisibilityObserver(path: Path, s3: S3Client, tag: String) extends AbstractS3VisibilityObserver(path) {
  override protected def makeTagRequest(bucket: String, key: String, visibility: String): Unit = {
    val tagging = Tagging.builder().tagSet(Tag.builder.key(tag).value(visibility).build()).build()
    val request = PutObjectTaggingRequest.builder.bucket(bucket).key(key).tagging(tagging).build()
    s3.putObjectTagging(request)
  }
}
