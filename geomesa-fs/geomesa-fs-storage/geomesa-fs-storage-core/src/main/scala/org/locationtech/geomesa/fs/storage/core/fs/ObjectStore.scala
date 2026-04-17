/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import java.io.{Closeable, InputStream, OutputStream}
import java.net.URI
import java.time.Instant

/**
 * Abstraction around object storage for data files
 */
trait ObjectStore extends Closeable {

  /**
   * Does an object exist at the given path
   *
   * @param path file path
   * @return
   */
  def exists(path: URI): Boolean

  /**
   * Get the size of the object stored at this path. If the file does not exist, an empty option will be returned
   *
   * @param path file path
   * @return
   */
  def size(path: URI): Option[Long]

  /**
   * Get the last modified date of the object stored at this path. If the file does not exist, an empty option will be returned
   *
   * @param path file path
   * @return
   */
  def modified(path: URI): Option[Long]

  /**
   * Write to the given path. If a file already exists at the path, an empty option will be returned
   *
   * @param path file path
   * @return output stream for writing to the file
   */
  def create(path: URI): Option[OutputStream]

  /**
   * Write to the given path. If a file already exists at the path, it will be overwritten
   *
   * @param path file path
   * @return output stream for writing to the file
   */
  def overwrite(path: URI): OutputStream

  /**
   * Reads the file at the given path. If the file does not exist, an empty option will be returned
   *
   * @param path file path
   * @return input stream for reading to the file
   */
  def read(path: URI): Option[InputStream]

  /**
   * List any files directly under the given directory path
   *
   * @param path directory path
   * @return
   */
  def list(path: URI): Seq[URI]

  /**
   * Copy a file
   *
   * @param from from
   * @param to to
   */
  def copy(from: URI, to: URI): Unit

  /**
   * Delete a file
   *
   * @param path file path
   */
  def delete(path: URI): Unit

//  def deleteRecursively(path: URI): Unit = ???
}

object ObjectStore {

  def apply(root: URI, conf: Map[String, String]): ObjectStore = {
    root.getScheme match {
      case null | "" => LocalObjectStore
      case "s3" | "s3a" => S3ObjectStore(conf)
      case scheme => throw new UnsupportedOperationException(s"No object store implemented for scheme: $scheme")
    }
  }
}
