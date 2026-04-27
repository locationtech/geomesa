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
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.checksums.{RequestChecksumCalculation, ResponseChecksumValidation}
import software.amazon.awssdk.core.retry.RetryMode
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import software.amazon.s3.analyticsaccelerator.{S3SdkObjectClient, S3SeekableInputStreamConfiguration, S3SeekableInputStreamFactory}

import java.io._
import java.net.URI
import java.nio.file.Files
import java.time.Duration
import java.util.Locale
import java.util.concurrent.CompletionException
import java.util.function.BiFunction
import scala.util.Try
import scala.util.control.NonFatal

/**
 * S3 object store implementation
 *
 * @param client s3 client
 * @param buffer write buffering strategy
 */
class S3ObjectStore(val client: S3AsyncClient, buffer: S3WriteBuffering) extends ObjectStore {

  import S3ObjectStore.parseS3Path

  import scala.collection.JavaConverters._

  private val transferManager = S3TransferManager.builder().s3Client(client).build()

  // accelerated input stream for working with parquet files
  val seekableInputStreamFactory: S3SeekableInputStreamFactory =
    new S3SeekableInputStreamFactory(new S3SdkObjectClient(client, false), S3SeekableInputStreamConfiguration.DEFAULT)

  override val scheme: String = "s3"

  override def exists(path: URI): Boolean = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    client.headObject(request).handle[Option[Boolean]](ifFound(_ => true)).join().getOrElse(false)
  }

  override def size(path: URI): Long = {
    val key = parseS3Path(path)
    val request =
      GetObjectAttributesRequest.builder().bucket(key.bucket).key(key.key).objectAttributes(ObjectAttributes.OBJECT_SIZE).build()
    client.getObjectAttributes(request).handle[Option[Long]](ifFound(_.objectSize())).join().getOrElse(0L)
  }

  override def modified(path: URI): Option[Long] = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    client.headObject(request).handle[Option[Long]](ifFound(_.lastModified().toEpochMilli)).join()
  }

  // note: since the write is not finalized until it is uploaded, this may return a Some but then throw an exception on close
  // if the path is written to after the existence check
  override def create(path: URI): Option[OutputStream] = {
    if (exists(path)) { None } else {
      val key = parseS3Path(path)
      Some(buffer.write(key.bucket, key.key, overwrite = false))
    }
  }

  override def overwrite(path: URI): OutputStream = {
    val key = parseS3Path(path)
    buffer.write(key.bucket, key.key, overwrite = true)
  }

  override def read(path: URI): Option[InputStream] = {
    val key = parseS3Path(path)
    val request =
      DownloadRequest.builder()
        .getObjectRequest(GetObjectRequest.builder().bucket(key.bucket).key(key.key).build())
        .responseTransformer(AsyncResponseTransformer.toBlockingInputStream())
        .build()
    transferManager.download(request).completionFuture()
      .handle[Option[InputStream]](ifFound(r => PathUtils.handleCompression(r.result(), path.toString)))
      .join()
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
    val response = client.listObjectsV2(request).join()
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
    client.copyObject(request).join()
  }

  override def delete(path: URI): Unit = {
    val key = parseS3Path(path)
    val request =
      DeleteObjectRequest.builder()
        .bucket(key.bucket)
        .key(key.key)
        .build()
    client.deleteObject(request).join()
  }

  override def close(): Unit = CloseWithLogging(client, transferManager)

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

    val buffering = try {
      config.writeBuffering match {
        case WriteBuffering.Disk => new DiskBuffering(client, bufferSize.toInt, new File(config.writeBufferDir))
        case WriteBuffering.Memory => new MemoryBuffering(client, bufferSize.toInt)
      }
    } catch {
      case NonFatal(e) => client.close(); throw e
    }

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
      "fs.s3a.access.key"         -> "fs.s3.access-key-id",
      "fs.s3a.secret.key"         -> "fs.s3.secret-access-key",
      "fs.s3a.endpoint"           -> "fs.s3.endpoint",
      "fs.s3a.endpoint.region"    -> "fs.s3.region",
      "fs.s3a.path.style.access"  -> "fs.s3.force-path-style",
      "fs.s3a.attempts.maximum"   -> "fs.s3.num-retries",
      "fs.s3a.connection.maximum" -> "fs.s3.max-concurrency", // TODO think max-concurrency is per-request
      "fs.s3a.connection.timeout" -> "fs.s3.connection-timeout",
      "fs.s3a.multipart.size"     -> "fs.s3.write-buffer-in-bytes",
      "fs.s3a.buffer.dir"         -> "fs.s3.write-buffer-dir"
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
