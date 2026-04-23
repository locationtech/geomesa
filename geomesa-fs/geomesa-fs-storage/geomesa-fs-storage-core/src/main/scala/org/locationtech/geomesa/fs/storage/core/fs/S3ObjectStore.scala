/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
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

class S3ObjectStore(val client: S3AsyncClient, buffer: S3WriteBuffering) extends ObjectStore {

  import S3ObjectStore.parseS3Path

  import scala.collection.JavaConverters._

  private val transferManager = S3TransferManager.builder().s3Client(client).build()

  val seekableInputStreamFactory: S3SeekableInputStreamFactory =
    new S3SeekableInputStreamFactory(new S3SdkObjectClient(client, false), S3SeekableInputStreamConfiguration.DEFAULT)

  override val scheme: String = "s3"

  override def exists(path: URI): Boolean = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    client.headObject(request).handle[Option[Boolean]](noSuchKey(_ => true)).join().getOrElse(false)
  }

  override def size(path: URI): Long = {
    val key = parseS3Path(path)
    val request =
      GetObjectAttributesRequest.builder().bucket(key.bucket).key(key.key).objectAttributes(ObjectAttributes.OBJECT_SIZE).build()
    client.getObjectAttributes(request).handle[Option[Long]](noSuchKey(_.objectSize())).join().getOrElse(0L)
  }

  override def modified(path: URI): Option[Long] = {
    val key = parseS3Path(path)
    val request = HeadObjectRequest.builder().bucket(key.bucket).key(key.key).build()
    client.headObject(request).handle[Option[Long]](noSuchKey(_.lastModified().toEpochMilli)).join()
  }

  override def create(path: URI): Option[OutputStream] = if (exists(path)) { None } else { Some(overwrite(path)) }

  override def overwrite(path: URI): OutputStream = {
    val key = parseS3Path(path)
    buffer.write(key.bucket, key.key)
  }

  override def read(path: URI): Option[InputStream] = {
    val key = parseS3Path(path)
    val request =
      DownloadRequest.builder()
        .getObjectRequest(GetObjectRequest.builder().bucket(key.bucket).key(key.key).build())
        .responseTransformer(AsyncResponseTransformer.toBlockingInputStream())
        .build()
    transferManager.download(request).completionFuture()
      .handle[Option[InputStream]](noSuchKey(r => PathUtils.handleCompression(r.result(), path.toString)))
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
  private def noSuchKey[T, U](resultTransform: T => U): BiFunction[T, Throwable, Option[U]] = { (result, error) =>
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

  private object S3ObjectStoreConfig {

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
      //  ).foreach { ms =>
      //    // this is supposed to be in ms but sometimes seems to include a unit?
      //    if (ms.matches("^[0-9]+$")) {
      //      s3aConfig.put("connection-timeout", s"${ms}ms")
      //    } else {
      //      s3aConfig.put("connection-timeout", ms)
      //    }
      //  }
    )

    def apply(conf: Map[String, String]): S3ObjectStoreConfig = {
      val s3 = conf.flatMap { case (k, v) => s3aConfigMappings.get(k).map(_ -> v) }.asJava
      val configSource =
        ConfigValueFactory.fromMap(conf.asJava).toConfig
          .withFallback(ConfigFactory.load())
          .withFallback(ConfigValueFactory.fromMap(s3))
          .withFallback(ConfigFactory.load("s3-defaults"))
          .resolve()
      ConfigSource.fromConfig(configSource).loadOrThrow[S3ObjectStoreConfig]
    }
  }
  //<property>
  //  <name>fs.s3a.aws.credentials.provider</name>
  //  <description>
  //    Comma-separated class names of credential provider classes which implement
  //    com.amazonaws.auth.AWSCredentialsProvider.
  //
  //    These are loaded and queried in sequence for a valid set of credentials.
  //    Each listed class must implement one of the following means of
  //    construction, which are attempted in order:
  //    1. a public constructor accepting java.net.URI and
  //        org.apache.hadoop.conf.Configuration,
  //    2. a public static method named getInstance that accepts no
  //       arguments and returns an instance of
  //       com.amazonaws.auth.AWSCredentialsProvider, or
  //    3. a public default constructor.
  //
  //    Specifying org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider allows
  //    anonymous access to a publicly accessible S3 bucket without any credentials.
  //    Please note that allowing anonymous access to an S3 bucket compromises
  //    security and therefore is unsuitable for most use cases. It can be useful
  //    for accessing public data sets without requiring AWS credentials.
  //
  //    If unspecified, then the default list of credential provider classes,
  //    queried in sequence, is:
  //    1. org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider:
  //       Uses the values of fs.s3a.access.key and fs.s3a.secret.key.
  //    2. com.amazonaws.auth.EnvironmentVariableCredentialsProvider: supports
  //        configuration of AWS access key ID and secret access key in
  //        environment variables named AWS_ACCESS_KEY_ID and
  //        AWS_SECRET_ACCESS_KEY, as documented in the AWS SDK.
  //    3. com.amazonaws.auth.InstanceProfileCredentialsProvider: supports use
  //        of instance profile credentials if running in an EC2 VM.
  //  </description>
  //</property>
  //<property>
  //  <name>fs.s3a.session.token</name>
  //  <description>
  //    Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
  //    as one of the providers.
  //  </description>
  //</property>
  //
  // <property>
  //  <name>fs.s3a.connection.maximum</name>
  //  <value>15</value>
  //  <description>Controls the maximum number of simultaneous connections to S3.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.connection.ssl.enabled</name>
  //  <value>true</value>
  //  <description>Enables or disables SSL connections to S3.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.host</name>
  //  <description>Hostname of the (optional) proxy server for S3 connections.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.port</name>
  //  <description>Proxy server port. If this property is not set
  //    but fs.s3a.proxy.host is, port 80 or 443 is assumed (consistent with
  //    the value of fs.s3a.connection.ssl.enabled).</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.username</name>
  //  <description>Username for authenticating with proxy server.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.password</name>
  //  <description>Password for authenticating with proxy server.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.domain</name>
  //  <description>Domain for authenticating with proxy server.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.proxy.workstation</name>
  //  <description>Workstation for authenticating with proxy server.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.attempts.maximum</name>
  //  <value>20</value>
  //  <description>How many times we should retry commands on transient errors.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.connection.establish.timeout</name>
  //  <value>5000</value>
  //  <description>Socket connection setup timeout in milliseconds.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.connection.timeout</name>
  //  <value>200000</value>
  //  <description>Socket connection timeout in milliseconds.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.paging.maximum</name>
  //  <value>5000</value>
  //  <description>How many keys to request from S3 when doing
  //     directory listings at a time.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.threads.max</name>
  //  <value>10</value>
  //  <description> Maximum number of concurrent active (part)uploads,
  //  which each use a thread from the threadpool.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.socket.send.buffer</name>
  //  <value>8192</value>
  //  <description>Socket send buffer hint to amazon connector. Represented in bytes.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.socket.recv.buffer</name>
  //  <value>8192</value>
  //  <description>Socket receive buffer hint to amazon connector. Represented in bytes.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.threads.keepalivetime</name>
  //  <value>60</value>
  //  <description>Number of seconds a thread can be idle before being
  //    terminated.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.max.total.tasks</name>
  //  <value>5</value>
  //  <description>Number of (part)uploads allowed to the queue before
  //  blocking additional uploads.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.size</name>
  //  <value>64M</value>
  //  <description>How big (in bytes) to split upload or copy operations up into.
  //    A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.threshold</name>
  //  <value>128MB</value>
  //  <description>How big (in bytes) to split upload or copy operations up into.
  //    This also controls the partition size in renamed files, as rename() involves
  //    copying the source file(s).
  //    A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multiobjectdelete.enable</name>
  //  <value>true</value>
  //  <description>When enabled, multiple single-object delete requests are replaced by
  //    a single 'delete multiple objects'-request, reducing the number of requests.
  //    Beware: legacy S3-compatible object stores might not support this request.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.acl.default</name>
  //  <description>Set a canned ACL for newly created and copied objects. Value may be Private,
  //    PublicRead, PublicReadWrite, AuthenticatedRead, LogDeliveryWrite, BucketOwnerRead,
  //    or BucketOwnerFullControl.
  //    If set, caller IAM role must have "s3:PutObjectAcl" permission on the bucket.
  //    </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.purge</name>
  //  <value>false</value>
  //  <description>True if you want to purge existing multipart uploads that may not have been
  //     completed/aborted correctly</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.purge.age</name>
  //  <value>86400</value>
  //  <description>Minimum age in seconds of multipart uploads to purge</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.signing-algorithm</name>
  //  <description>Override the default signing algorithm so legacy
  //    implementations can still be used</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.encryption.algorithm</name>
  //  <description>Specify a server-side encryption or client-side
  //     encryption algorithm for s3a: file system. Unset by default. It supports the
  //     following values: 'AES256' (for SSE-S3), 'SSE-KMS', 'SSE-C', and 'CSE-KMS'
  //  </description>
  //</property>
  //
  //<property>
  //    <name>fs.s3a.encryption.key</name>
  //    <description>Specific encryption key to use if fs.s3a.encryption.algorithm
  //        has been set to 'SSE-KMS', 'SSE-C' or 'CSE-KMS'. In the case of SSE-C
  //    , the value of this property should be the Base64 encoded key. If you are
  //     using SSE-KMS and leave this property empty, you'll be using your default's
  //     S3 KMS key, otherwise you should set this property to the specific KMS key
  //     id. In case of 'CSE-KMS' this value needs to be the AWS-KMS Key ID
  //     generated from AWS console.
  //    </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.buffer.dir</name>
  //  <value>${hadoop.tmp.dir}/s3a</value>
  //  <description>Comma separated list of directories that will be used to buffer file
  //    uploads to.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.block.size</name>
  //  <value>32M</value>
  //  <description>Block size to use when reading files using s3a: file system.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.user.agent.prefix</name>
  //  <value></value>
  //  <description>
  //    Sets a custom value that will be prepended to the User-Agent header sent in
  //    HTTP requests to the S3 back-end by S3AFileSystem.  The User-Agent header
  //    always includes the Hadoop version number followed by a string generated by
  //    the AWS SDK.  An example is "User-Agent: Hadoop 2.8.0, aws-sdk-java/1.10.6".
  //    If this optional property is set, then its value is prepended to create a
  //    customized User-Agent.  For example, if this configuration property was set
  //    to "MyApp", then an example of the resulting User-Agent would be
  //    "User-Agent: MyApp, Hadoop 2.8.0, aws-sdk-java/1.10.6".
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.impl</name>
  //  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  //  <description>The implementation class of the S3A Filesystem</description>
  //</property>
  //
  //<property>
  //  <name>fs.AbstractFileSystem.s3a.impl</name>
  //  <value>org.apache.hadoop.fs.s3a.S3A</value>
  //  <description>The implementation class of the S3A AbstractFileSystem.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.readahead.range</name>
  //  <value>64K</value>
  //  <description>Bytes to read ahead during a seek() before closing and
  //  re-opening the S3 HTTP connection. This option will be overridden if
  //  any call to setReadahead() is made to an open stream.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.input.async.drain.threshold</name>
  //  <value>64K</value>
  //  <description>Bytes to read ahead during a seek() before closing and
  //  re-opening the S3 HTTP connection. This option will be overridden if
  //  any call to setReadahead() is made to an open stream.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.list.version</name>
  //  <value>2</value>
  //  <description>Select which version of the S3 SDK's List Objects API to use.
  //  Currently support 2 (default) and 1 (older API).</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.connection.request.timeout</name>
  //  <value>0</value>
  //  <description>
  //  Time out on HTTP requests to the AWS service; 0 means no timeout.
  //  Measured in seconds; the usual time suffixes are all supported
  //
  //  Important: this is the maximum duration of any AWS service call,
  //  including upload and copy operations. If non-zero, it must be larger
  //  than the time to upload multi-megabyte blocks to S3 from the client,
  //  and to rename many-GB files. Use with care.
  //
  //  Values that are larger than Integer.MAX_VALUE milliseconds are
  //  converged to Integer.MAX_VALUE milliseconds
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.bucket.probe</name>
  //  <value>0</value>
  //  <description>
  //     The value can be 0 (default), 1 or 2.
  //     When set to 0, bucket existence checks won't be done
  //     during initialization thus making it faster.
  //     Though it should be noted that when the bucket is not available in S3,
  //     or if fs.s3a.endpoint points to the wrong instance of a private S3 store
  //     consecutive calls like listing, read, write etc. will fail with
  //     an UnknownStoreException.
  //     When set to 1, the bucket existence check will be done using the
  //     V1 API of the S3 protocol which doesn't verify the client's permissions
  //     to list or read data in the bucket.
  //     When set to 2, the bucket existence check will be done using the
  //     V2 API of the S3 protocol which does verify that the
  //     client has permission to read the bucket.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.object.content.encoding</name>
  //  <value></value>
  //  <description>
  //    Content encoding: gzip, deflate, compress, br, etc.
  //    This will be set in the "Content-Encoding" header of the object,
  //    and returned in HTTP HEAD/GET requests.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.create.storage.class</name>
  //  <value></value>
  //  <description>
  //      Storage class: standard, reduced_redundancy, intelligent_tiering, etc.
  //      Specify the storage class for S3A PUT object requests.
  //      If not set the storage class will be null
  //      and mapped to default standard class on S3.
  //  </description>
  //</property>
  // <property>
  //  <name>fs.s3a.retry.throttle.limit</name>
  //  <value>${fs.s3a.attempts.maximum}</value>
  //  <description>
  //    Number of times to retry any throttled request.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.retry.throttle.interval</name>
  //  <value>1000ms</value>
  //  <description>
  //    Interval between retry attempts on throttled requests.
  //  </description>
  //</property>
  // <property>
  //    <name>fs.s3a.bucket.sample-bucket.accesspoint.arn</name>
  //    <value> {ACCESSPOINT_ARN_HERE} </value>
  //    <description>Configure S3a traffic to use this AccessPoint</description>
  //</property>
  //This configures access to the sample-bucket bucket for S3A, to go through the new Access Point ARN. So, for example s3a://sample-bucket/key will now use your configured ARN when getting data from S3 instead of your bucket.
  //
  //The fs.s3a.accesspoint.required property can also require all access to S3 to go through Access Points. This has the advantage of increasing security inside a VPN / VPC as you only allow access to known sources of data defined through Access Points. In case there is a need to access a bucket directly (without Access Points) then you can use per bucket overrides to disable this setting on a bucket by bucket basis i.e. fs.s3a.bucket.{YOUR-BUCKET}.accesspoint.required.
  //
  //<!-- Require access point only access -->
  //<property>
  //    <name>fs.s3a.accesspoint.required</name>
  //    <value>true</value>
  //</property>
  //<!-- Disable it on a per-bucket basis if needed -->
  //<property>
  //    <name>fs.s3a.bucket.example-bucket.accesspoint.required</name>
  //    <value>false</value>
  //</property>
  //<property>
  //    <name>fs.s3a.requester.pays.enabled</name>
  //    <value>true</value>
  //</property>
  // <property>
  //    <name>fs.s3a.create.storage.class</name>
  //    <value>intelligent_tiering</value>
  //</property>
  // <property>
  //  <name>fs.s3a.fast.upload.buffer</name>
  //  <value>disk</value>
  //  <description>
  //    The buffering mechanism to use.
  //    Values: disk, array, bytebuffer.
  //
  //    "disk" will use the directories listed in fs.s3a.buffer.dir as
  //    the location(s) to save data prior to being uploaded.
  //
  //    "array" uses arrays in the JVM heap
  //
  //    "bytebuffer" uses off-heap memory within the JVM.
  //
  //    Both "array" and "bytebuffer" will consume memory in a single stream up to the number
  //    of blocks set by:
  //
  //        fs.s3a.multipart.size * fs.s3a.fast.upload.active.blocks.
  //
  //    If using either of these mechanisms, keep this value low
  //
  //    The total number of threads performing work across all threads is set by
  //    fs.s3a.threads.max, with fs.s3a.max.total.tasks values setting the number of queued
  //    work items.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.size</name>
  //  <value>100M</value>
  //  <description>How big (in bytes) to split upload or copy operations up into.
  //    A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.fast.upload.active.blocks</name>
  //  <value>8</value>
  //  <description>
  //    Maximum Number of blocks a single output stream can have
  //    active (uploading, or queued to the central FileSystem
  //    instance's pool of queued operations.
  //
  //    This stops a single stream overloading the shared thread pool.
  //  </description>
  //</property>
  // <property>
  //  <name>fs.s3a.fast.upload.buffer</name>
  //  <value>disk</value>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.buffer.dir</name>
  //  <value>${hadoop.tmp.dir}/s3a</value>
  //  <description>Comma separated list of directories that will be used to buffer file
  //    uploads to.</description>
  //</property>
  // <property>
  //  <name>fs.s3a.fast.upload.buffer</name>
  //  <value>bytebuffer</value>
  //</property>
  //Buffering upload data in byte arrays: fs.s3a.fast.upload.buffer=array
  //When fs.s3a.fast.upload.buffer is set to array, all data is buffered in byte arrays in the JVM’s heap prior to upload. This may be faster than buffering to disk.
  //
  //The amount of data which can be buffered is limited by the available size of the JVM heap heap. The slower the write bandwidth to S3, the greater the risk of heap overflows. This risk can be mitigated by tuning the upload settings.
  //
  //<property>
  //  <name>fs.s3a.fast.upload.buffer</name>
  //  <value>array</value>
  //</property>
  // <property>
  //  <name>fs.s3a.fast.upload.active.blocks</name>
  //  <value>4</value>
  //  <description>
  //    Maximum Number of blocks a single output stream can have
  //    active (uploading, or queued to the central FileSystem
  //    instance's pool of queued operations.
  //
  //    This stops a single stream overloading the shared thread pool.
  //  </description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.threads.max</name>
  //  <value>10</value>
  //  <description>The total number of threads available in the filesystem for data
  //    uploads *or any other queued filesystem operation*.</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.max.total.tasks</name>
  //  <value>5</value>
  //  <description>The number of operations which can be queued for execution</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.threads.keepalivetime</name>
  //  <value>60</value>
  //  <description>Number of seconds a thread can be idle before being
  //    terminated.</description>
  //</property>
  // <property>
  //  <name>fs.s3a.multipart.purge</name>
  //  <value>true</value>
  //  <description>True if you want to purge existing multipart uploads that may not have been
  //     completed/aborted correctly</description>
  //</property>
  //
  //<property>
  //  <name>fs.s3a.multipart.purge.age</name>
  //  <value>86400</value>
  //  <description>Minimum age in seconds of multipart uploads to purge</description>
  //</property>
  // <property>
  //  <name>fs.s3a.etag.checksum.enabled</name>
  //  <value>false</value>
  //  <description>
  //    Should calls to getFileChecksum() return the etag value of the remote
  //    object.
  //    WARNING: if enabled, distcp operations between HDFS and S3 will fail unless
  //    -skipcrccheck is set.
  //  </description>
  //</property>
}