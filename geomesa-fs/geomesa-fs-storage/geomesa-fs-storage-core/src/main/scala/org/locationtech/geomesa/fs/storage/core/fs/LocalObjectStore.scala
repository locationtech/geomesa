/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.ArchiveFormat.ArchiveFormat
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.{ArchiveFormat, NamedInputStream}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.{ArchiveFileIterator, ZipFileIterator}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import java.io._
import java.net.URI

object LocalObjectStore extends ObjectStore {

  private lazy val archiveFactory = new ArchiveStreamFactory()

  override def exists(path: URI): Boolean = new File(path.toString).exists()

  override def size(path: URI): Long = {
    val file = new File(path.toString)
    if (file.exists()) { file.length() } else { 0L }
  }

  override def modified(path: URI): Option[Long] = Option(new File(path.toString)).collect { case f if f.exists() => f.lastModified() }

  override def create(path: URI): Option[OutputStream] = Option(new File(path.toString)).collect { case f if !f.exists() => write(f) }

  override def overwrite(path: URI): OutputStream = write(new File(path.toString))

  private def write(file: File): OutputStream = {
    if (file.getParentFile != null) {
      file.getParentFile.mkdirs()
    }
    new FileOutputStream(file)
  }

  override def read(path: URI): Option[InputStream] =
    Option(new File(path.toString)).collect { case f if f.exists() => new FileInputStream(f) }

  override def read(path: URI, format: ArchiveFormat): CloseableIterator[NamedInputStream] = {
    val iter = format match {
      case ArchiveFormat.Tar =>
        CloseableIterator(read(path).iterator).flatMap { is =>
          val uncompressed = PathUtils.handleCompression(is, path.toString)
          val archive: ArchiveInputStream[_ <: ArchiveEntry] =
            archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
          new ArchiveFileIterator(archive, path.toString)
        }

      case ArchiveFormat.Zip =>
        CloseableIterator.single(new File(path.toString)).filter(_.exists()).flatMap { file =>
          new ZipFileIterator(new ZipFile(file), path.toString)
        }

      case _ =>
        throw new UnsupportedOperationException(s"An implementation is missing for format $format")
    }
    iter.map { case (name, is) => NamedInputStream(name, is) }
  }

  override def list(path: URI): CloseableIterator[URI] = {
    val file = new File(path.toString)
    if (!file.isDirectory) {
      return CloseableIterator.empty
    }
    CloseableIterator(file.listFiles().map(_.toURI).iterator)
  }

  override def copy(from: URI, to: URI): Unit =
    WithClose(new FileInputStream(new File(from.toString)), new FileOutputStream(new File(to.toString)))(IOUtils.copy)

  override def delete(path: URI): Unit = new File(path.toString).delete()

  override def close(): Unit = {}
}
