/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.model._

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer

/**
 * Trait for buffering writes to s3
 */
trait S3WriteBuffering {

  /**
   * Gets an output stream for writing to S3
   *
   * @param fs object store
   * @param bucket bucket
   * @param key key
   * @param overwrite overwrite any existing object
   * @return
   */
  def write(fs: S3ObjectStore, bucket: String, key: String, overwrite: Boolean): OutputStream
}

object S3WriteBuffering {

  /**
   * Disk write buffering
   *
   * @param bufferSize buffer size, in bytes
   * @param dir tmp dir used for buffering
   */
  class DiskBuffering(bufferSize: Int, dir: File) extends S3WriteBuffering {

    override def write(fs: S3ObjectStore, bucket: String, key: String, overwrite: Boolean): OutputStream =
      new DiskOutputStream(fs, bucket, key, overwrite)

    private class DiskOutputStream(
        fs: S3ObjectStore,
        bucket: String,
        key: String,
        overwrite: Boolean,
        uid: String = UUID.randomUUID().toString.substring(0, 8),
        private var i: Int = 0
      ) extends ChunkedOutputStream(fs, bucket, key, bufferSize, overwrite) {
      override protected def newChunk(): Chunk = {
        val path = s"${bucket.replaceAll("[^a-zA-Z0-9_-]", "_")}/${key.replaceAll("[^a-zA-Z0-9_-]", "_")}/part_${uid}_$i"
        val file = new File(dir, path)
        file.getParentFile.mkdirs()
        i += 1
        new DiskChunk(file)
      }
    }

    private class DiskChunk(file: File) extends Chunk(new BufferedOutputStream(new FileOutputStream(file))) {
      override def read: AsyncRequestBody = AsyncRequestBody.fromFile(file)
      override def cleanup(): Unit = file.delete()
    }
  }

  /**
   * Off-heap memory buffering
   *
   * @param bufferSize buffer size, in bytes
   */
  class MemoryBuffering(bufferSize: Int) extends S3WriteBuffering {

    override def write(fs: S3ObjectStore, bucket: String, key: String, overwrite: Boolean): OutputStream =
      new MemoryOutputStream(fs, bucket, key, overwrite)

    private class MemoryOutputStream(fs: S3ObjectStore, bucket: String, key: String, overwrite: Boolean)
        extends ChunkedOutputStream(fs, bucket, key, bufferSize, overwrite) {
      override protected def newChunk(): Chunk = new MemoryChunk(ByteBuffer.allocateDirect(bufferSize))
    }

    private class MemoryChunk(buf: ByteBuffer) extends Chunk(new ByteBufferOutputStream(buf)) {
      override def read: AsyncRequestBody = {
        buf.rewind()
        AsyncRequestBody.fromRemainingByteBuffersUnsafe(buf)
      }
      override def cleanup(): Unit = {}
    }

    private class ByteBufferOutputStream(buf: ByteBuffer) extends OutputStream {
      override def write(data: Int): Unit = buf.put(data.toByte)
      override def write(data: Array[Byte]): Unit = buf.put(data)
      override def write(data: Array[Byte], off: Int, len: Int): Unit = buf.put(data, off, len)
      override def flush(): Unit = {}
      override def close(): Unit = {}
    }
  }

  private abstract class ChunkedOutputStream(fs: S3ObjectStore, bucket: String, key: String, bufferSize: Int, overwrite: Boolean)
      extends OutputStream with LazyLogging {

    private var currentChunk = newChunk()
    private var currentChunkSize = 0
    private var uploadId: String = _
    private val uploads = ArrayBuffer.empty[CompletableFuture[CompletedPart]]

    override def write(data: Int): Unit = {
      currentChunk.os.write(data)
      currentChunkSize += 1
      if (currentChunkSize >= bufferSize) {
        closeChunk()
      }
    }

    override def write(data: Array[Byte]): Unit = {
      currentChunk.os.write(data)
      currentChunkSize += data.length
      if (currentChunkSize >= bufferSize) {
        closeChunk()
      }
    }

    override def write(data: Array[Byte], off: Int, len: Int): Unit = {
      currentChunk.os.write(data, off, len)
      currentChunkSize += len
      if (currentChunkSize >= bufferSize) {
        closeChunk()
      }
    }

    override def flush(): Unit = currentChunk.os.flush()

    override def close(): Unit = closeChunk(true)

    private def closeChunk(done: Boolean = false): Unit = {
      currentChunk.os.close()
      if (done && uploadId == null) {
        // didn't hit our multipart threshold, just use a regular put object
        logger.debug(s"Starting regular s3 upload to $bucket/$key")
        fs.put(bucket, key, currentChunk.read, overwrite)
        logger.debug(s"Completed regular s3 upload to $bucket/$key")
      } else {
        if (uploadId == null) {
          logger.debug(s"Starting multipart upload to $bucket/$key")
          uploadId = fs.createMultipartUpload(bucket, key)
          logger.debug(s"Initiated multipart upload with id $uploadId to $bucket/$key")
        }
        val chunk = currentChunk
        val future = retry(chunk, uploads.size + 1, 2)
        uploads += future
        future.whenCompleteAsync((_, _) => chunk.cleanup())
        logger.debug(s"Initiated part ${uploads.size} for upload with id $uploadId to $bucket/$key")
        if (done) {
          val parts = uploads.map(_.join()).toSeq
          fs.completeMultipartUpload(bucket, key, uploadId, parts, overwrite)
          logger.debug(s"Completed multipart upload with id $uploadId consisting of ${uploads.size} parts to $bucket/$key")
        } else {
          currentChunk = newChunk()
          currentChunkSize = 0
        }
      }
    }

    private def retry(currentChunk: Chunk, partNumber: Int, retries: Int): CompletableFuture[CompletedPart] = {
      fs.uploadPart(bucket, key, uploadId, partNumber, currentChunk.read).exceptionallyCompose { e =>
        if (retries > 0) {
          logger.warn(s"Error with multipart upload, retrying $retries more times:", e)
          retry(currentChunk, partNumber, retries - 1)
        } else {
          logger.error("Error with multipart upload:", e)
          throw e
        }
      }
    }

    protected def newChunk(): Chunk
  }

  private abstract class Chunk(val os: OutputStream) {
    def read: AsyncRequestBody
    def cleanup(): Unit
  }
}
