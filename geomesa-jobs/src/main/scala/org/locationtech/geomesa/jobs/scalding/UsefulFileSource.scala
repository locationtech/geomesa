/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.util.Properties
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import cascading.flow.FlowProcess
import cascading.scheme.{NullScheme, Scheme}
import cascading.tap.local.FileTap
import cascading.tap.{SinkMode, Tap}
import cascading.tuple.{TupleEntryCollector, TupleEntryIterator}
import com.twitter.scalding._
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream, BZip2Utils}
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.{XZCompressorInputStream, XZCompressorOutputStream, XZUtils}
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import org.locationtech.geomesa.jobs.scalding.UsefulFileTap._

class UsefulFileSource(path: String*) extends FixedPathSource(path: _*) {

  // Hint+FYI: LineRecordReader understands codecs...so in hdfs mode
  // we get gzip ingest for free but the local tap from scalding
  // does not understand codecs so overriding this method gives us local
  // ingest for gzip
  override def createLocalTap(sinkMode: SinkMode): Tap[_, _, _] =
    new UsefulFileTap(localScheme, localPath, sinkMode)

}

class UsefulFileTap(scheme: Scheme[Properties, InputStream, OutputStream, _, _],
                    path: String,
                    sinkMode: SinkMode = SinkMode.KEEP)
  extends FileTap(scheme, path, sinkMode) {

  val codec =
    path match {
      case _ if GzipUtils.isCompressedFilename(path)  => GZ
      case _ if BZip2Utils.isCompressedFilename(path) => BZ
      case _ if XZUtils.isCompressedFilename(path)    => XZ
      case _ => "none"
    }

  override def openForRead(flowProcess: FlowProcess[Properties], input: InputStream): TupleEntryIterator =
    super.openForRead(flowProcess, if (input == null) getInputStream() else input)

  override def openForWrite(flowProcess: FlowProcess[Properties], output: OutputStream): TupleEntryCollector =
    super.openForWrite(flowProcess, if (output == null) getOutputStream() else output)

  def getInputStream(): InputStream =
    codec match {
      case GZ => new GZIPInputStream(new FileInputStream(getIdentifier))
      case BZ => new BZip2CompressorInputStream(new FileInputStream(getIdentifier))
      case XZ => new XZCompressorInputStream(new FileInputStream(getIdentifier))
      case _  => new FileInputStream(getIdentifier)
    }

  def getOutputStream(): OutputStream =
    codec match {
      case GZ => new GZIPOutputStream(new FileOutputStream(getIdentifier))
      case BZ => new BZip2CompressorOutputStream(new FileOutputStream(getIdentifier))
      case XZ => new XZCompressorOutputStream(new FileOutputStream(getIdentifier))
      case _  => new FileOutputStream(getIdentifier)
    }
}

object UsefulFileTap {
  val GZ = "gz"
  val BZ = "bz"
  val XZ = "xz"
}

// TODO enable usage of multiple local files: GEOMESA-593
case class MultipleUsefulTextLineFiles(path: String*) extends UsefulFileSource(path: _*) with TextLineScheme
