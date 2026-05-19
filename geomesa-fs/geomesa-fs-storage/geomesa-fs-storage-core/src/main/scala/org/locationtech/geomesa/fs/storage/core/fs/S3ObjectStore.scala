/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.ArchiveFormat.ArchiveFormat
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore.{ArchiveFormat, NamedInputStream}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore.WriteBuffering.WriteBuffering
import org.locationtech.geomesa.fs.storage.core.fs.S3WriteBuffering.{DiskBuffering, MemoryBuffering}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.{ArchiveFileIterator, ZipFileIterator}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils, WithClose}
import pureconfig._
import pureconfig.error.CannotConvert
import pureconfig.generic.ProductHint
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.core.checksums.{RequestChecksumCalculation, ResponseChecksumValidation}
import software.amazon.awssdk.core.retry.RetryMode
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import software.amazon.s3.analyticsaccelerator.util.S3URI
import software.amazon.s3.analyticsaccelerator.{S3SdkObjectClient, S3SeekableInputStream, S3SeekableInputStreamConfiguration, S3SeekableInputStreamFactory}

import java.io._
import java.net.URI
import java.nio.file.Files
import java.time.Duration
import java.util.Locale
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiFunction
import scala.util.Try

/**
 * S3 object store implementation
 *
 * @param client s3 client
 * @param buffer write buffering strategy
 */
class S3ObjectStore(client: S3AsyncClient, buffer: S3WriteBuffering) extends ObjectStore with LazyLogging {

  import S3ObjectStore.parseS3Path

  import scala.collection.JavaConverters._

  // we need to ensure we don't invoke methods on a closed s3 client, as that can cause segfaults
  // we do this with 2 variables -
  // `open` tracks how many open calls we have, and allows us to wait for them to complete before closing
  // `closed` prevents us from opening new calls once `close()` has been called

  @volatile
  private var closed = false
  private val open = new Phaser(1)

  private val transferManager = S3TransferManager.builder().s3Client(client).build()

  // accelerated input stream for working with parquet files
  private val seekableInputStreamFactory =
    new S3SeekableInputStreamFactory(new S3SdkObjectClient(client, false), S3SeekableInputStreamConfiguration.DEFAULT)

  override val scheme: String = "s3"

  /**
   * Wrapper to ensure our client is a: open and b: won't be closed until the call completes
   *
   * @param fn function to run using the s3 client
   * @return
   */
  private def ensureOpen[T](fn: => T): T = {
    val deregister = ensureOpen()
    try { fn } finally {
      deregister()
    }
  }

  /**
   * Alternate method for ensuring open, when the connection is closed externally
   *
   * @return
   */
  private def ensureOpen(): DeregisterOnce = {
    // note: the closed check is so that we don't initiate new calls after closing but before all open connections spin down
    // since we're not synchronized, it's possible a call will slip through after close() is called,
    // but then we'll just wait for it based on the phaser anyway
    if (closed || open.register() < 0) {
      throw new IllegalStateException("This instance has been closed")
    }
    new DeregisterOnce()
  }

  // start of s3-specific methods, proxied here to ensure we can manage calls based on the open/closed state of the client

  /**
   * Create a seekable input stream for reading from S3
   *
   * @param bucket bucket
   * @param key object key
   * @return
   */
  def createStream(bucket: String, key: String): S3SeekableInputStream = {
    // TODO we can't easily hook into the close method to deregister the open connection
    ensureOpen(seekableInputStreamFactory.createStream(S3URI.of(bucket, key)))
  }

  /**
   * Put an object
   *
   * @param bucket bucket
   * @param key key
   * @param body object
   * @param overwrite overwrite any existing object
   */
  def putObject(bucket: String, key: String, body: AsyncRequestBody, overwrite: Boolean = true): Unit = {
    val request = PutObjectRequest.builder().bucket(bucket).key(key)
    if (!overwrite) {
      request.ifNoneMatch("*")
    }
    ensureOpen(client.putObject(request.build(), body).join())
  }

  /**
   * Start a new multipart upload
   *
   * @param bucket bucket
   * @param key key
   * @return upload id
   */
  def createMultipartUpload(bucket: String, key: String): String =
    ensureOpen(client.createMultipartUpload(b => b.bucket(bucket).key(key)).join().uploadId())

  /**
   * Upload a part of a multipart upload
   *
   * @param bucket bucket
   * @param key key
   * @param uploadId multipart upload id
   * @param partNumber part number
   * @param body object part
   * @return
   */
  def uploadPart(bucket: String, key: String, uploadId: String, partNumber: Int, body: AsyncRequestBody): CompletableFuture[CompletedPart] = {
    val request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
    val deregister = ensureOpen()
    try {
      client.uploadPart(request, body)
        .whenComplete((_, _) => deregister())
        .thenApply(r => CompletedPart.builder().partNumber(partNumber).eTag(r.eTag()).build())
    } catch {
      case e: Throwable => deregister(); throw e
    }
  }

  /**
   *
   * @param bucket bucket
   * @param key key
   * @param uploadId multipart upload id
   */
  def completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: Seq[CompletedPart], overwrite: Boolean = true): Unit = {
    val request =
      CompleteMultipartUploadRequest.builder()
        .bucket(bucket).key(key).uploadId(uploadId).multipartUpload(b => b.parts(parts.asJava))
    if (!overwrite) {
      request.ifNoneMatch("*")
    }
    ensureOpen(client.completeMultipartUpload(request.build()).join())
  }

  /**
   * Tag an object
   *
   * @param bucket bucket
   * @param key key
   * @param tags key-value tag pairs
   */
  def putObjectTagging(bucket: String, key: String, tags: Seq[(String, String)]): Unit = {
    val tagSet = tags.map { case (k, v) => Tag.builder.key(k).value(v).build() }
    val tagging = Tagging.builder().tagSet(tagSet.asJava).build()
    val request = PutObjectTaggingRequest.builder.bucket(bucket).key(key).tagging(tagging).build()
    ensureOpen(client.putObjectTagging(request).join())
  }

  // end of s3-specific methods

  override def exists(path: URI): Boolean = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    ensureOpen(client.headObject(request).handle[Option[Boolean]](ifFound(_ => true)).join().getOrElse(false))
  }

  override def size(path: URI): Long = {
    val key = parseS3Path(path)
    val request =
      GetObjectAttributesRequest.builder().bucket(key.bucket).key(key.key).objectAttributes(ObjectAttributes.OBJECT_SIZE).build()
    ensureOpen(client.getObjectAttributes(request).handle[Option[Long]](ifFound(_.objectSize())).join().getOrElse(0L))
  }

  override def modified(path: URI): Option[Long] = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    ensureOpen(client.headObject(request).handle[Option[Long]](ifFound(_.lastModified().toEpochMilli)).join())
  }

  // note: since the write is not finalized until it is uploaded, this may return a Some but then throw an exception on close
  // if the path is written to after the existence check
  override def create(path: URI): Option[OutputStream] = {
    if (exists(path)) { None } else {
      val key = parseS3Path(path)
      Some(buffer.write(this, key.bucket, key.key, overwrite = false))
    }
  }

  override def overwrite(path: URI): OutputStream = {
    val key = parseS3Path(path)
    buffer.write(this, key.bucket, key.key, overwrite = true)
  }

  override def read(path: URI): Option[InputStream] = {
    val key = parseS3Path(path)
    val request =
      DownloadRequest.builder()
        .getObjectRequest(GetObjectRequest.builder().bucket(key.bucket).key(key.key).build())
        .responseTransformer(AsyncResponseTransformer.toBlockingInputStream())
        .build()
    val deregister = ensureOpen()
    val result =
      try {
        transferManager.download(request).completionFuture()
          .handle[Option[InputStream]](ifFound(r => PathUtils.handleCompression(r.result(), path.toString)))
          .join()
      } catch {
        case e: Throwable => deregister(); throw e
      }
    result match {
      case None =>
        deregister()
        None
      case Some(is) =>
        val wrapped = new FilterInputStream(is) {
          override def close(): Unit = {
            deregister()
            super.close()
          }
        }
        Some(wrapped)
    }
  }

  override def read(path: URI, format: ArchiveFormat): CloseableIterator[NamedInputStream] = {
    val iter = format match {
      case ArchiveFormat.Tar =>
        CloseableIterator(read(path).iterator).flatMap { is =>
          val archive: ArchiveInputStream[_ <: ArchiveEntry] =
            S3ObjectStore.archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, is)
          new ArchiveFileIterator(archive, path.toString)
        }

      case ArchiveFormat.Zip =>
        // we have to download the file to get random access reads
        // note: there is a potential way to wrap the request in a Seekable Channel using
        // https://github.com/awslabs/aws-java-nio-spi-for-s3/, but it doesn't seem worthwhile assuming
        // we end up reading the whole file
        // another potential option is to wrap an S3SeekableInputStream
        val name = filename(path)
        val tmp = Files.createTempFile(FilenameUtils.removeExtension(name), FilenameUtils.getExtension(name))
        CloseableIterator(read(path).iterator).flatMap { is =>
          WithClose(is)(IOUtils.copy(_, new FileOutputStream(tmp.toFile)))
          val zipIter = new ZipFileIterator(new ZipFile(tmp), path.toString)
          new CloseableIterator[(String, InputStream)]() {
            override def hasNext: Boolean = zipIter.hasNext
            override def next(): (String, InputStream) = zipIter.next()
            override def close(): Unit = {
              try { zipIter.close() } finally {
                Files.delete(tmp)
              }
            }
          }
        }

      case _ =>
        throw new UnsupportedOperationException(s"An implementation is missing for format $format")
    }
    iter.map { case (name, is) => NamedInputStream(name, is) }
  }

  override def list(path: URI): CloseableIterator[URI] = {
    val key = parseS3Path(path)
    // ensure prefix ends with '/' to target only items inside it
    val prefix = if (key.key.endsWith("/")) { key.key } else { key.key + "/" }
    list(path.getScheme, key.bucket, prefix, null)
  }

  private def list(scheme: String, bucket: String, prefix: String, continuation: String): CloseableIterator[URI] = {
    val request =
      ListObjectsV2Request.builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/") // prevents listing more than 1 directory deep
        .continuationToken(continuation)
        .build()
    val response = ensureOpen(client.listObjectsV2(request).join())
    val files = response.contents().asScala.map(_.key())
    val folders = response.commonPrefixes().asScala.map(_.prefix())
    val results = CloseableIterator.wrap(files ++ folders).map(key => new URI(s"$scheme://$bucket/$key"))
    if (response.isTruncated) {
      results.concat(list(scheme, bucket, prefix, response.continuationToken()))
    } else {
      results
    }
  }

  override def copy(from: URI, to: URI): Unit = {
    val fromKey = parseS3Path(from)
    val toKey = parseS3Path(to)
    val request =
      CopyObjectRequest.builder()
        .sourceBucket(fromKey.bucket)
        .sourceKey(fromKey.key)
        .destinationBucket(toKey.bucket)
        .destinationKey(toKey.key)
        .build()
    ensureOpen(client.copyObject(request).join())
  }

  override def delete(path: URI): Unit = {
    val key = parseS3Path(path)
    val request =
      DeleteObjectRequest.builder()
        .bucket(key.bucket)
        .key(key.key)
        .build()
    ensureOpen(client.deleteObject(request).join())
  }

  override def close(): Unit = {
    closed = true // set closed true first, this prevents any new attempts to register against the phaser
    val phase = open.arriveAndDeregister() // arrive, this will terminate the phaser once all registered parties complete
    // wait for any currently registered parties to complete
    try { open.awaitAdvanceInterruptibly(phase, 1L, TimeUnit.SECONDS) } catch {
      case _: Throwable => logger.warn(s"Closing S3Client with ${open.getRegisteredParties} operations in flight")
    }
    if (!open.isTerminated) {
      // this might prevent some clients from starting calls depending on threading order (if they've already cleared
      // the `closed` check but haven't yet registered to the phaser)
      open.forceTermination()
    }
    CloseWithLogging(Seq(seekableInputStreamFactory, transferManager, client))
  }

  /**
   * Helper method to handle missing key errors in completable futures
   *
   * @param resultTransform success result
   * @return
   */
  private def ifFound[T, U](resultTransform: T => U): BiFunction[T, Throwable, Option[U]] = { (result, error) =>
    if (error == null) {
      Option(resultTransform(result))
    } else {
      val unwrapped = error match {
        case e: CompletionException => e.getCause
        case _ => error
      }
      unwrapped match {
        case e: S3Exception if e.statusCode() == 404 => None
        case _: NoSuchKeyException => None
        case _ => throw error
      }
    }
  }

  /**
   * Ensures we don't deregister more than once, which is a logical error and may cause issues later on
   */
  private class DeregisterOnce extends (() => Unit) {
    private val deregistered = new AtomicBoolean(false)
    override def apply(): Unit = if (deregistered.compareAndSet(false, true)) { open.arriveAndDeregister() }
  }
}

object S3ObjectStore {

  val Type = "s3"

  private val archiveFactory = new ArchiveStreamFactory()

  def parseS3Path(path: URI): S3Path = {
    if (path.getScheme != "s3" && path.getScheme != "s3a") {
      throw new UnsupportedOperationException(s"Trying to use S3 to operate on a non-s3 URI: $path")
    }
    S3Path(path.getHost, path.getPath.stripPrefix("/"))
  }

  /**
   * Create a new S3 store
   *
   * @param conf config
   * @return
   */
  def apply(conf: Map[String, String]): S3ObjectStore = {
    val config = S3ObjectStoreConfig(conf)
    val bufferSize = org.locationtech.geomesa.utils.text.Suffixes.Memory.bytes(config.writeBufferInBytes).get
    if (bufferSize > Int.MaxValue) {
      throw new IllegalArgumentException(s"Buffer size exceeds max size in bytes (${Int.MaxValue}b): $bufferSize")
    }

    val buffering = config.writeBuffering match {
      case WriteBuffering.Disk => new DiskBuffering(bufferSize.toInt, new File(config.writeBufferDir))
      case WriteBuffering.Memory => new MemoryBuffering(bufferSize.toInt)
    }

    val builder = S3AsyncClient.crtBuilder()
    // or AWS_REGION
    config.region.foreach(builder.region)
    // endpoint override (for MinIO, LocalStack, or other S3-compatible services)
    config.endpoint.foreach(builder.endpointOverride)
    // AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or ~/.aws/credentials
    config.accessKeyId.foreach { accessKeyId =>
      config.secretAccessKey.foreach { secretAccessKey =>
        builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
      }
    }
    if (config.forcePathStyle) {
      builder.forcePathStyle(true) // path-style access, often needed for MinIO/LocalStack
    }

    config.numRetries.foreach(m => builder.retryConfiguration(c => c.numRetries(m)))
    config.targetThroughputInGbps.foreach(builder.targetThroughputInGbps(_))
    config.minimumPartSizeInBytes.foreach(builder.minimumPartSizeInBytes(_))
    config.maxConcurrency.foreach(builder.maxConcurrency(_))
    config.connectionTimeout.foreach(t => builder.httpConfiguration(c => c.connectionTimeout(t)))
    config.maxNativeMemoryLimitInBytes.foreach(builder.maxNativeMemoryLimitInBytes(_))
    config.requestChecksumCalculation.foreach(builder.requestChecksumCalculation)
    config.responseChecksumValidation.foreach(builder.responseChecksumValidation)
    config.initialReadBufferSizeInBytes.foreach(builder.initialReadBufferSizeInBytes(_))
    config.accelerate.foreach(builder.accelerate(_))
    config.thresholdInBytes.foreach(builder.thresholdInBytes(_))

    val client = builder.build()

    new S3ObjectStore(client, buffering)
  }

  /**
   * Converts geomesa s3-related configuration values to hadoop s3a ones
   *
   * @param conf geomesa configuration
   * @return
   */
  def s3aConfigs(conf: Map[String, String]): Map[String, String] =
    conf.flatMap { case (k, v) => S3ObjectStoreConfig.s3aReverseConfigMappings.get(k).map(_ -> v )}

  /**
   * If the URI has an 's3://' scheme, converts it to 's3a://' for use with hadoop
   *
   * @param path path
   * @return
   */
  def s3aUri(path: URI): URI = {
    if (path.getScheme != "s3") { path } else {
      new URI("s3a", path.getHost, path.getPath, path.getFragment)
    }
  }

  /**
   * Write buffering strategies
   */
  object WriteBuffering extends Enumeration {
    type WriteBuffering = Value
    val Disk, Memory = Value
  }

  case class S3Path(bucket: String, key: String)

  /**
   * Configuration keys accepted by the S3ObjectStore
   */
  object S3Config {
    // note: these need to stay in sync with s3-defaults.conf
    val Endpoint                     = "fs.s3.endpoint"
    val Region                       = "fs.s3.region"
    val AccessKeyId                  = "fs.s3.access-key-id"
    val SecretAccessKey              = "fs.s3.secret-access-key"
    val ForcePathStyle               = "fs.s3.force-path-style"
    val WriteBuffering               = "fs.s3.write-buffering"
    val WriteBufferDir               = "fs.s3.write-buffer-dir"
    val WriteBufferInBytes           = "fs.s3.write-buffer-in-bytes"
    val NumRetries                   = "fs.s3.num-retries"
    val TargetThroughputInGbps       = "fs.s3.target-throughput-in-gbps"
    val MinimumPartSizeInBytes       = "fs.s3.minimum-part-size-in-bytes"
    val MaxConcurrency               = "fs.s3.max-concurrency"
    val ConnectionTimeout            = "fs.s3.connection-timeout"
    val MaxNativeMemoryLimitInBytes  = "fs.s3.max-native-memory-limit-in-bytes"
    val RequestChecksumCalculation   = "fs.s3.request-checksum-calculation"
    val ResponseChecksumValidation   = "fs.s3.response-checksum-validation"
    val InitialReadBufferSizeInBytes = "fs.s3.initial-read-buffer-size-in-bytes"
    val Accelerate                   = "fs.s3.accelerate"
    val ThresholdInBytes             = "fs.s3.threshold-in-bytes"
  }

  // TODO checksum-enabled on upload ?
  private case class S3ObjectStoreConfig(
    region: Option[Region],
    endpoint: Option[URI],
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    forcePathStyle: Boolean,
    numRetries: Option[Int],
    targetThroughputInGbps: Option[Double],
    minimumPartSizeInBytes: Option[Long],
    maxConcurrency: Option[Int],
    connectionTimeout: Option[Duration],
    maxNativeMemoryLimitInBytes: Option[Long],
    requestChecksumCalculation: Option[RequestChecksumCalculation],
    responseChecksumValidation: Option[ResponseChecksumValidation],
    initialReadBufferSizeInBytes: Option[Long],
    accelerate: Option[Boolean],
    thresholdInBytes: Option[Long],
    writeBuffering: WriteBuffering,
    writeBufferDir: String,
    writeBufferInBytes: String,
  )

  private object S3ObjectStoreConfig extends LazyLogging {

    import pureconfig.generic.auto._

    import scala.collection.JavaConverters._

    // noinspection ScalaUnusedSymbol
    private implicit val productHint: ProductHint[S3ObjectStoreConfig] = {
      val baseMapping = ConfigFieldMapping(CamelCase, KebabCase).withOverrides("s3Endpoint" -> "s3-endpoint")
      val mapping = ConfigFieldMapping(s => "fs.s3." + baseMapping(s))
      ProductHint[S3ObjectStoreConfig](mapping)
    }

    // noinspection ScalaUnusedSymbol
    private implicit val awsRegionConfigReader: ConfigReader[Region] =
      ConfigReader.fromString { s =>
        Try(Region.of(s)).toEither
          .left.map(_ => CannotConvert(s, "Region", s"Must be one of: ${Region.regions().asScala.mkString(", ")}"))
      }

    // noinspection ScalaUnusedSymbol
    private implicit val retryModeConfigReader: ConfigReader[RetryMode] =
      enumReader(classOf[RetryMode])

    // noinspection ScalaUnusedSymbol
    private implicit val requestChecksumCalculationConfigReader: ConfigReader[RequestChecksumCalculation] =
      enumReader(classOf[RequestChecksumCalculation])

    // noinspection ScalaUnusedSymbol
    private implicit val responseChecksumCalculationConfigReader: ConfigReader[ResponseChecksumValidation] =
      enumReader(classOf[ResponseChecksumValidation])

    // noinspection ScalaUnusedSymbol
    private implicit val javaDurationConfigReader: ConfigReader[Duration] =
      ConfigReader.fromNonEmptyString[Duration](s =>
        Try(Duration.ofMillis(scala.concurrent.duration.Duration(s).toMillis))
          .toEither.left.map(e => CannotConvert(s, classOf[Duration].getName, e.toString)))

    // noinspection ScalaUnusedSymbol
    private implicit val writeBufferingReader: ConfigReader[WriteBuffering] =
      ConfigReader.fromString { s =>
        WriteBuffering.values.find(_.toString.equalsIgnoreCase(s)).toRight {
          CannotConvert(s, "WriteBuffering", s"Must be one of: ${WriteBuffering.values.mkString(", ")}")
        }
      }

    private def enumReader[T <: Enum[T]](clas: Class[T]): ConfigReader[T] = {
      ConfigReader.fromString { s =>
        Try(Enum.valueOf[T](clas, s.toUpperCase(Locale.US))).toEither
          .left.map(_ => CannotConvert(s, clas.getSimpleName, s"Must be one of: ${clas.getEnumConstants.mkString(", ")}"))
      }
    }

    private val s3aConfigMappings = Map(
      "fs.s3a.access.key"         -> S3Config.AccessKeyId,
      "fs.s3a.secret.key"         -> S3Config.SecretAccessKey,
      "fs.s3a.endpoint"           -> S3Config.Endpoint,
      "fs.s3a.endpoint.region"    -> S3Config.Region,
      "fs.s3a.path.style.access"  -> S3Config.ForcePathStyle,
      "fs.s3a.attempts.maximum"   -> S3Config.NumRetries,
      "fs.s3a.connection.maximum" -> S3Config.MaxConcurrency, // TODO think max-concurrency is per-request
      "fs.s3a.connection.timeout" -> S3Config.ConnectionTimeout,
      "fs.s3a.multipart.size"     -> S3Config.WriteBufferInBytes,
      "fs.s3a.buffer.dir"         -> S3Config.WriteBufferDir,
    )

    val s3aReverseConfigMappings: Map[String, String] = s3aConfigMappings.map { case (k, v) => v -> k }

    def apply(conf: Map[String, String]): S3ObjectStoreConfig = {
      val s3 = conf.flatMap { case (k, v) => s3aConfigMappings.get(k).map(_ -> v) }.asJava
      val configSource =
        ConfigValueFactory.fromMap(conf.asJava).toConfig
          .withFallback(ConfigFactory.load())
          .withFallback(ConfigValueFactory.fromMap(s3))
          .withFallback(ConfigFactory.load("s3-defaults"))
          .resolve()
      val config = ConfigSource.fromConfig(configSource).loadOrThrow[S3ObjectStoreConfig]
      logger.debug(
        s"""S3 client configuration:
           |  region=${config.region.getOrElse("")}
           |  endpoint=${config.endpoint.getOrElse("")}
           |  accessKeyId=${config.accessKeyId.fold("")(_ => "***")}
           |  secretAccessKey=${config.secretAccessKey.fold("")(_ => "***")}
           |  forcePathStyle=${config.forcePathStyle}
           |  numRetries=${config.numRetries.getOrElse("")}
           |  targetThroughputInGbps=${config.targetThroughputInGbps.getOrElse("")}
           |  minimumPartSizeInBytes=${config.minimumPartSizeInBytes.getOrElse("")}
           |  maxConcurrency=${config.maxConcurrency.getOrElse("")}
           |  connectionTimeout=${config.connectionTimeout.getOrElse("")}
           |  maxNativeMemoryLimitInBytes=${config.maxNativeMemoryLimitInBytes.getOrElse("")}
           |  requestChecksumCalculation=${config.requestChecksumCalculation.getOrElse("")}
           |  responseChecksumValidation=${config.responseChecksumValidation.getOrElse("")}
           |  initialReadBufferSizeInBytes=${config.initialReadBufferSizeInBytes.getOrElse("")}
           |  accelerate=${config.accelerate.getOrElse("")}
           |  thresholdInBytes=${config.thresholdInBytes.getOrElse("")}
           |  writeBuffering=${config.writeBuffering}
           |  writeBufferDir=${config.writeBufferDir}
           |  writeBufferInBytes=${config.writeBufferInBytes}""".stripMargin
      )
      config
    }
  }
}
