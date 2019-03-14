/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.mutable.ListBuffer

package object fs {

  class ZipFileIterator(zipfile: ZipFile, path: String) extends CloseableIterator[(Option[String], InputStream)]() {

    private val entries = zipfile.getEntries
    private val open = ListBuffer.empty[InputStream]
    private var entry: ZipArchiveEntry = _

    override def hasNext: Boolean = {
      if (entry == null && entries.hasMoreElements) {
        entry = entries.nextElement()
        while (entry != null && (entry.isDirectory || !zipfile.canReadEntryData(entry))) {
          entry = if (entries.hasMoreElements) { entries.nextElement() } else { null }
        }
      }
      entry != null
    }

    override def next(): (Option[String], InputStream) = {
      if (!hasNext) { Iterator.empty.next() } else {
        try {
          val is = zipfile.getInputStream(entry)
          open += is
          Some(s"$path/${entry.getName}") -> is
        } finally {
          entry = null
        }
      }
    }

    override def close(): Unit = {
      CloseWithLogging(open)
      CloseWithLogging(zipfile)
    }
  }

  class ArchiveFileIterator(archive: ArchiveInputStream, path: String)
      extends CloseableIterator[(Option[String], InputStream)]() {

    // the archive input stream will only read the current entry
    // but we need to wrap it to prevent it from being closed before reading all entries
    private val wrapper = new ArchiveInputStreamWrapper(archive)
    private var entry: ArchiveEntry = _

    override def hasNext: Boolean = {
      if (entry == null) {
        entry = archive.getNextEntry
        while (entry != null && (entry.isDirectory || !archive.canReadEntryData(entry))) {
          entry = archive.getNextEntry
        }
      }
      entry != null
    }

    override def next(): (Option[String], InputStream) = {
      if (!hasNext) { Iterator.empty.next() } else {
        try { Some(s"$path/${entry.getName}") -> wrapper } finally {
          entry = null
        }
      }
    }

    override def close(): Unit = archive.close()
  }

  class ArchiveInputStreamWrapper(is: InputStream) extends InputStream {
    override def read(): Int = is.read()
    override def read(b: Array[Byte]): Int = is.read(b)
    override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    override def skip(n: Long): Long = is.skip(n)
    override def available(): Int = is.available()
    override def mark(readlimit: Int): Unit = is.mark(readlimit)
    override def markSupported(): Boolean = is.markSupported()
    override def reset(): Unit = is.reset()

    // override close to prevent closing the underlying stream
    override def close(): Unit = {}
  }
}
