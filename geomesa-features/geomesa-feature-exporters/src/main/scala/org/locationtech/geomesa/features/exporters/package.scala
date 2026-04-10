/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.apache.commons.io.output.CountingOutputStream
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.{CreateMode, FileHandle}

import java.io.{BufferedOutputStream, ByteArrayOutputStream, OutputStream}
import java.util.zip.GZIPOutputStream

package object exporters {

  /**
   * Output stream that counts bytes written
   */
  trait ByteCounterStream extends OutputStream {

    /**
     * How many bytes have been written to this stream
     * @return
     */
    def bytes: Long
  }

  /**
   * Export stream that writes to a byte array
   */
  class ByteArrayExportStream extends ByteCounterStream {
    private val os = new ByteArrayOutputStream()
    override def bytes: Long = os.size()
    override def write(b: Int): Unit = os.write(b)
    def toByteArray: Array[Byte] = os.toByteArray
  }

  /**
   * Export output stream
   *
   * @param out file handle
   * @param gzip gzip
   */
  class ExportStream(out: FileHandle, gzip: Option[Int] = None) extends ByteCounterStream {

    // lowest level - keep track of the bytes we write
    // do this before any compression, buffering, etc so we get an accurate count
    private val counter = new CountingOutputStream(out.write(CreateMode.Create))
    private val stream = {
      val compressed = gzip match {
        case None => counter
        case Some(c) => new GZIPOutputStream(counter) { `def`.setLevel(c) } // hack to access the protected deflate level
      }
      new BufferedOutputStream(compressed)
    }

    override def bytes: Long = counter.getByteCount

    override def write(b: Array[Byte]): Unit = stream.write(b)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = stream.write(b, off, len)
    override def write(b: Int): Unit = stream.write(b)

    override def flush(): Unit = stream.flush()
    override def close(): Unit = stream.close()
  }

  /**
   * Export output stream, lazily instantiates the file when features are written
   *
   * @param out file handle
   * @param gzip gzip
   */
  class LazyExportStream(out: FileHandle, gzip: Option[Int] = None) extends ByteCounterStream {

    private var stream: ExportStream = _

    private def ensureStream(): OutputStream = {
      if (stream == null) {
        stream = new ExportStream(out, gzip)
      }
      stream
    }

    override def bytes: Long = if (stream == null) { 0L } else { stream.bytes }

    override def write(b: Array[Byte]): Unit = ensureStream().write(b)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ensureStream().write(b, off, len)
    override def write(b: Int): Unit = ensureStream().write(b)

    override def flush(): Unit = if (stream != null) { stream.flush() }
    override def close(): Unit = if (stream != null) { stream.close() }
  }
}
