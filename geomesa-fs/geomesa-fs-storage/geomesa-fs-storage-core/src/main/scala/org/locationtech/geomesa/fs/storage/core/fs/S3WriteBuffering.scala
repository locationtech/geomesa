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
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompletedPart, PutObjectRequest, UploadPartRequest, UploadPartResponse}

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
   * @param bucket bucket
   * @param key key
   * @return
   */
  def write(bucket: String, key: String): OutputStream
}

object S3WriteBuffering {

  /**
   * Disk write buffering
   *
   * @param client s3 client
   * @param bufferSize buffer size, in bytes
   * @param dir tmp dir used for buffering
   */
  class DiskBuffering(client: S3AsyncClient, bufferSize: Int, dir: File) extends S3WriteBuffering {

    override def write(bucket: String, key: String): OutputStream = new DiskOutputStream(bucket, key)

    private class DiskOutputStream(bucket: String, key: String) extends ChunkedOutputStream(client, bucket, key, bufferSize) {
      private val uid = UUID.randomUUID().toString.substring(0, 8)
      private var i = 0
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
   * @param client s3 client
   * @param bufferSize buffer size, in bytes
   */
  class MemoryBuffering(client: S3AsyncClient, bufferSize: Int) extends S3WriteBuffering {

    override def write(bucket: String, key: String): OutputStream = new MemoryOutputStream(bucket, key)

    private class MemoryOutputStream(bucket: String, key: String) extends ChunkedOutputStream(client, bucket, key, bufferSize) {
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

  private abstract class ChunkedOutputStream(client: S3AsyncClient, bucket: String, key: String, bufferSize: Int)
      extends OutputStream with LazyLogging {

    import scala.collection.JavaConverters._

    private var currentChunk = newChunk()
    private var currentChunkSize = 0
    private var uploadId: String = _
    private val uploads = ArrayBuffer.empty[CompletableFuture[UploadPartResponse]]

    override def write(data: Int): Unit = {
      currentChunkSize += 1
      currentChunk.os.write(data)
      if (currentChunkSize >= bufferSize) {
        closeChunk()
      }
    }

    override def write(data: Array[Byte]): Unit = {
      currentChunkSize += data.length
      currentChunk.os.write(data)
      if (currentChunkSize >= bufferSize) {
        closeChunk()
      }
    }

    override def write(data: Array[Byte], off: Int, len: Int): Unit = {
      currentChunkSize += len
      currentChunk.os.write(data, off, len)
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
        val request = PutObjectRequest.builder().bucket(bucket).key(key).build()
        client.putObject(request, currentChunk.read).join()
        logger.debug(s"Completed regular s3 upload to $bucket/$key")
      } else {
        if (uploadId == null) {
          logger.debug(s"Starting multipart upload to $bucket/$key")
          uploadId = client.createMultipartUpload(b => b.bucket(bucket).key(key)).join().uploadId()
          logger.debug(s"Initiated multipart upload with id $uploadId to $bucket/$key")
        }
        val future = retry(currentChunk, uploads.size + 1, 2)
        uploads += future
        future.whenCompleteAsync { case (_, _) => currentChunk.cleanup() }
        logger.debug(s"Initiated part ${uploads.size} for upload with id $uploadId to $bucket/$key")
        if (done) {
          var partNumber = 0
          val parts = uploads.map { future =>
            partNumber += 1
            val response = future.join()
            CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
          }
          val request =
            CompleteMultipartUploadRequest.builder()
              .bucket(bucket).key(key).uploadId(uploadId).multipartUpload(b => b.parts(parts.asJava)).build()
          client.completeMultipartUpload(request).join()
          logger.debug(s"Completed multipart upload with id $uploadId consisting of ${uploads.size} parts to $bucket/$key")
        } else {
          currentChunk = newChunk()
          currentChunkSize = 0
        }
      }
    }

    private def retry(currentChunk: Chunk, partNumber: Int, retries: Int): CompletableFuture[UploadPartResponse] = {
      val request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
      client.uploadPart(request, currentChunk.read).exceptionallyCompose { e =>
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
