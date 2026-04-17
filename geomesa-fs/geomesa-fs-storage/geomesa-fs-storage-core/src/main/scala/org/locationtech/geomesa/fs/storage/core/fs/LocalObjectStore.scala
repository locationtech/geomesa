/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.utils.io.WithClose

import java.io._
import java.net.URI

object LocalObjectStore extends ObjectStore {

  override def exists(path: URI): Boolean = new File(path).exists()

  override def size(path: URI): Option[Long] = Option(new File(path)).collect { case f if f.exists() => f.length() }

  override def modified(path: URI): Option[Long] = Option(new File(path)).collect { case f if f.exists() => f.lastModified() }

  override def create(path: URI): Option[OutputStream] = Option(new File(path)).collect { case f if !f.exists() => write(f) }

  override def overwrite(path: URI): OutputStream = write(new File(path))

  private def write(file: File): OutputStream = {
    if (file.getParentFile != null) {
      file.getParentFile.mkdirs()
    }
    new FileOutputStream(file)
  }

  override def read(path: URI): Option[InputStream] =
    Option(new File(path)).collect { case f if f.exists() => new FileInputStream(f) }

  override def list(path: URI): Seq[URI] = {
    val file = new File(path)
    if (!file.isDirectory) {
      return Seq.empty
    }
    file.listFiles().map(_.toURI).toSeq
  }

  override def copy(from: URI, to: URI): Unit =
    WithClose(new FileInputStream(new File(from)), new FileOutputStream(new File(to)))(IOUtils.copy)

  override def delete(path: URI): Unit = new File(path).delete()

  override def close(): Unit = {}
}
