/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io.s3

import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.io.{InputFile, ParquetFileRange, SeekableInputStream}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.parquet.io.s3.S3InputFile.S3SeekableInputStream
import software.amazon.s3.analyticsaccelerator.common.ObjectRange
import software.amazon.s3.analyticsaccelerator.util.S3URI

import java.io.{EOFException, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.function.{Consumer, IntFunction}

/**
 * Parquet input file implementation for reading from S3. Uses the aws-s3-analyticsaccelerator project to improve
 * read times.
 *
 * @param fs object store impl
 * @param path file path
 */
class S3InputFile(fs: S3ObjectStore, path: URI) extends InputFile {

  private val key = S3ObjectStore.parseS3Path(path)

  override lazy val getLength: Long = fs.size(path)

  override def newStream(): SeekableInputStream =
    new S3SeekableInputStream(fs.seekableInputStreamFactory.createStream(S3URI.of(key.bucket, key.key)))
}

object S3InputFile {

  class S3SeekableInputStream(s3: software.amazon.s3.analyticsaccelerator.S3SeekableInputStream) extends SeekableInputStream {

    import scala.collection.JavaConverters._

    private lazy val buf = Array.ofDim[Byte](1024)

    override def getPos: Long = s3.getPos

    override def seek(newPos: Long): Unit = s3.seek(newPos)

    override def read(): Int = s3.read()

    override def read(buf: ByteBuffer): Int = {
      val max = math.min(buf.remaining(), this.buf.length)
      val count = s3.read(this.buf, 0, max)
      if (count > 0) {
        buf.put(this.buf, 0, count)
      }
      count
    }

    override def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)

    override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = s3.readFully(s3.getPos, bytes, start, len)

    override def readFully(buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0 && read(buf) != -1) {
      }
      if (buf.remaining() > 0) {
        throw new EOFException()
      }
    }

    override def read(b: Array[Byte]): Int = s3.read(b)

    override def read(b: Array[Byte], off: Int, len: Int): Int = s3.read(b, off, len)

    override def readAllBytes(): Array[Byte] = s3.readAllBytes()

    override def readNBytes(len: Int): Array[Byte] = s3.readNBytes(len)

    override def readNBytes(b: Array[Byte], off: Int, len: Int): Int = s3.readNBytes(b, off, len)

    override def skip(n: Long): Long = s3.skip(n)

    override def skipNBytes(n: Long): Unit = s3.skipNBytes(n)

    override def available(): Int = s3.available()

    override def mark(readlimit: Int): Unit = s3.mark(readlimit)

    override def reset(): Unit = s3.reset()

    override def markSupported(): Boolean = s3.markSupported()

    override def transferTo(out: OutputStream): Long = s3.transferTo(out)

    override def readVectoredAvailable(allocator: ByteBufferAllocator): Boolean = true

    override def readVectored(ranges: java.util.List[ParquetFileRange], allocator: ByteBufferAllocator): Unit = {
      val allocate: IntFunction[ByteBuffer] = size => allocator.allocate(size)
      val release: Consumer[ByteBuffer] = buf => allocator.release(buf)
      val objectRanges = new java.util.ArrayList[ObjectRange](ranges.size())
      ranges.asScala.foreach { range =>
        val result = new CompletableFuture[ByteBuffer]()
        objectRanges.add(new ObjectRange(result, range.getOffset, range.getLength))
        range.setDataReadFuture(result)
      }
      s3.readVectored(objectRanges, allocate, release)
    }

    override def close(): Unit = s3.close()
  }
}
