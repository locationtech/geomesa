/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import software.amazon.awssdk.services.s3.S3AsyncClient

import java.io.{InputStream, OutputStream}
import java.net.URI

class S3ObjectStore(val s3Client: S3AsyncClient) extends ObjectStore {

  override def exists(path: URI): Boolean = ???

  override def size(path: URI): Option[Long] = ???

  override def modified(path: URI): Option[Long] = ???

  override def create(path: URI): Option[OutputStream] = ???

  override def overwrite(path: URI): OutputStream = ???

  override def read(path: URI): Option[InputStream] = ???

  override def list(path: URI): Seq[URI] = ???

  override def copy(from: URI, to: URI): Unit = ???

  override def delete(path: URI): Unit = ???

  override def close(): Unit = s3Client.close()
}

object S3ObjectStore {
  def apply(conf: Map[String, String]): S3ObjectStore = ???
}