/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.{BufferedOutputStream, ByteArrayOutputStream, Closeable, OutputStream}
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.utils.CountingOutputStream
import org.locationtech.geomesa.tools.`export`.formats.FeatureExporter.ByteCounter
import org.locationtech.geomesa.utils.io.PathUtils
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.CreateMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Exports features in various formats. Usage pattern is:
  *
  *   start()
  *   export() - 0 to n times
  *   close()
  */
trait FeatureExporter extends ByteCounter with Closeable {

  /**
    * Start the export
    *
    * @param sft simple feature type
    */
  def start(sft: SimpleFeatureType): Unit

  /**
    * Export a batch of features
    *
    * @param features features to export
    * @return count of features exported, if available
    */
  def export(features: Iterator[SimpleFeature]): Option[Long]
}

object FeatureExporter {

  /**
    * Counts bytes written so far
    */
  trait ByteCounter {

    /**
     * Number of bytes written so far (including buffered output).
     *
     * Note that this may be expensive to calculate.
     *
     * @return
     */
    def bytes: Long
  }

  /**
    * Feature exporter with a delegate byte counter
    *
    * @param stream output stream
    */
  abstract class ByteCounterExporter(stream: ExportStream) extends FeatureExporter {
    override def bytes: Long = stream.bytes
  }

  /**
   * Abstraction around export streams
   */
  trait ExportStream extends ByteCounter with Closeable {
    def os: OutputStream
  }

  /**
   * Export output stream, lazily instantiated
   *
   * @param name file name
   * @param gzip gzip
   */
  class LazyExportStream(name: Option[String], gzip: Option[Int]) extends ExportStream {

    // lowest level - keep track of the bytes we write
    // do this before any compression, buffering, etc so we get an accurate count
    private var counter: CountingOutputStream = _
    private var stream: OutputStream = _

    override def os: OutputStream = {
      if (stream == null) {
        val base = name match {
          case None    => System.out
          case Some(n) => PathUtils.getHandle(n).write(CreateMode.Create, createParents = true)
        }
        counter = new CountingOutputStream(base)
        val compressed = gzip match {
          case None => counter
          case Some(c) => new GZIPOutputStream(counter) { `def`.setLevel(c) } // hack to access the protected deflate level
        }
        stream = new BufferedOutputStream(compressed)
      }
      stream
    }

    override def bytes: Long = if (counter == null) { 0L } else { counter.getBytesWritten }

    override def close(): Unit = if (stream != null) { stream.close() }
  }

  /**
   * Byte output stream, mainly for testing
   */
  class ByteExportStream extends ExportStream {
    override val os: ByteArrayOutputStream = new ByteArrayOutputStream()
    override def bytes: Long = os.size()
    override def close(): Unit = {}
    def toByteArray: Array[Byte] = os.toByteArray
  }
}
