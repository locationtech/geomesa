/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream

/**
  * Common trait for Apache compression utilities. Each compression type (gzip, xz, bzip) has the same
  * static method signatures, but they do not implement a common interface. This trait unifies them, so
  * that code can just deal with a CompressionUtils instance and not have to explicitly deal with each
  * compression type
  */
trait CompressionUtils {
  def isCompressedFilename(filename: String): Boolean
  def getUncompressedFilename(filename: String): String
  def getCompressedFilename(filename: String): String
  def compress(is: InputStream): InputStream
}

object CompressionUtils {

  val Utils: Seq[CompressionUtils] = Seq(GzipUtils, XZUtils, BZip2Utils)

  /**
    * Gets the uncompressed file name. E.g. 'foo.tgz -> foo.tar', 'foo.tar.gz -> foo.tar', etc
    *
    * @param filename file name
    * @return
    */
  def getUncompressedFilename(filename: String): String = {
    val uncompressed = Utils.collectFirst {
      case u if u.isCompressedFilename(filename) => u.getUncompressedFilename(filename)
    }
    uncompressed.getOrElse(filename)
  }

  /**
    * Does the filename indicate compression? E.g. 'foo.tgz', 'foo.tar.gz', etc
    *
    * @param filename file name
    * @return
    */
  def isCompressedFilename(filename: String): Boolean = Utils.exists(_.isCompressedFilename(filename))

  case object GzipUtils extends CompressionUtils {
    override def isCompressedFilename(filename: String): Boolean =
      org.apache.commons.compress.compressors.gzip.GzipUtils.isCompressedFilename(filename)
    override def getUncompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.gzip.GzipUtils.getUncompressedFilename(filename)
    override def getCompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.gzip.GzipUtils.getCompressedFilename(filename)
    override def compress(is: InputStream): InputStream = new GZIPInputStream(is)
  }

  case object XZUtils extends CompressionUtils {
    override def isCompressedFilename(filename: String): Boolean =
      org.apache.commons.compress.compressors.xz.XZUtils.isCompressedFilename(filename)
    override def getUncompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.xz.XZUtils.getUncompressedFilename(filename)
    override def getCompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.xz.XZUtils.getCompressedFilename(filename)
    override def compress(is: InputStream): InputStream = new XZCompressorInputStream(is)
  }

  case object BZip2Utils extends CompressionUtils {
    override def isCompressedFilename(filename: String): Boolean =
      org.apache.commons.compress.compressors.bzip2.BZip2Utils.isCompressedFilename(filename)
    override def getUncompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.bzip2.BZip2Utils.getUncompressedFilename(filename)
    override def getCompressedFilename(filename: String): String =
      org.apache.commons.compress.compressors.bzip2.BZip2Utils.getCompressedFilename(filename)
    override def compress(is: InputStream): InputStream = new BZip2CompressorInputStream(is)
  }
}
