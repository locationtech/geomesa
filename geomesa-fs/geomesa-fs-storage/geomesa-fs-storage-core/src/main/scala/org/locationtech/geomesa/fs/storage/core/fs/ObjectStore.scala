/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import org.apache.commons.compress.archivers.ArchiveStreamFactory.{JAR, TAR, ZIP}
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.ArchiveFormat.ArchiveFormat
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.{ArchiveFormat, NamedInputStream}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.PathUtils

import java.io.{Closeable, InputStream, OutputStream}
import java.net.URI
import java.util.Locale

/**
 * Abstraction around object storage for data files
 */
trait ObjectStore extends Closeable {

  /**
   * Default URI scheme
   *
   * @return
   */
  def scheme: String

  /**
   * Does an object exist at the given path
   *
   * @param path file path
   * @return
   */
  def exists(path: URI): Boolean

  /**
   * Get the size of the object stored at this path. If the file does not exist, 0L will be returned
   *
   * @param path file path
   * @return
   */
  def size(path: URI): Long

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
   * Gets the archive format of a file, if any.
   *
   * Currently, this is just based on the file name
   *
   * @param path file path
   * @return
   */
  def format(path: URI): Option[ArchiveFormat] = {
    PathUtils.getUncompressedExtension(path.toString).toLowerCase(Locale.US) match {
      case TAR => Some(ArchiveFormat.Tar)
      case ZIP | JAR => Some(ArchiveFormat.Zip)
      case _ => None
    }
  }

  /**
   * Reads the file at the given path. If the file does not exist, an empty option will be returned
   *
   * @param path file path
   * @return input stream for reading to the file
   */
  def read(path: URI): Option[InputStream]

  /**
   * Reads an archive file (tar, zip or jar) into its constituent files
   *
   * @param path file path
   * @return
   */
  def read(path: URI, format: ArchiveFormat): CloseableIterator[NamedInputStream]

  /**
   * List any files directly under the given directory path
   *
   * @param path directory path
   * @return
   */
  def list(path: URI): CloseableIterator[URI]

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

  /**
   * Utility method to return the "name" of a file
   *
   * @param path path
   * @return
   */
  def filename(path: URI): String = filename(path.toString)

  private def filename(path: String): String = {
    val i = path.lastIndexOf('/')
    if (i == -1) {
      path
    } else if (i == path.length - 1) {
      filename(path.substring(0, path.length - 1))
    } else {
      path.substring(i + 1)
    }
  }
}

object ObjectStore {

  def apply(context: FileSystemContext): ObjectStore = {
    context.root.getScheme match {
      case "file" => LocalObjectStore
      case "s3" | "s3a" => S3ObjectStore(context.conf)
      case scheme => throw new UnsupportedOperationException(s"No object store implemented for scheme: $scheme")
    }
  }

  object ArchiveFormat extends Enumeration {
    type ArchiveFormat = Value
    val Tar, Zip = Value
  }

  case class NamedInputStream(name: String, is: InputStream)
}
