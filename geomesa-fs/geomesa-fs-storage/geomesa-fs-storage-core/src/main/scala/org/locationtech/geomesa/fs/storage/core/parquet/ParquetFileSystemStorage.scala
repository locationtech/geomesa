/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package parquet

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.iceberg.DataFile
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.calrissian.mango.types.TypeEncoder
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileType.FileType
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemPathReader, FileSystemUpdateWriter, FileSystemWriter, FileType}
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.ColumnBounds
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.parquet.io.{ParquetFileSystemReader, ParquetFileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.fs.storage.core.utils.{FileSize, FileSystemThreadedReader}
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, FileValidationEnabled, Partition, StorageMetadata}
import org.locationtech.geomesa.security.{AuthProviderParam, AuthUtils, AuthorizationsProvider, AuthsParam, VisibilityUtils}
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, FlushQuietly, WithClose}

import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Persists simple features to parquet and provides query access
 *
 * @param context handle to the file context, root path and configuration
 * @param metadata metadata on files for this instance
 */
class ParquetFileSystemStorage(val context: FileSystemContext, val metadata: StorageMetadata) extends FileSystemStorage {

  import org.locationtech.geomesa.fs.storage.core.parquet.ParquetFileSystemStorage._

  import scala.collection.JavaConverters._

  // don't require observers if we never write any data
  lazy private val observers = {
    val builder = Seq.newBuilder[FileSystemObserverFactory]
    metadata.sft.getObservers.foreach { c =>
      try {
        // use the context classloader if defined, so that child classloaders can be accessed, as per SPI loading
        val cl = Option(Thread.currentThread.getContextClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
        // noinspection ScalaDeprecation
        val observer = cl.loadClass(c).getDeclaredConstructor().newInstance() match {
          case o: FileSystemObserverFactory => o
          case o => throw new IllegalArgumentException(s"Expected a FileSystemObserverFactory but got: ${o.getClass.getName}")
        }
        builder += observer
        observer.init(this)
      } catch {
        case NonFatal(e) => CloseQuietly(builder.result).foreach(e.addSuppressed); throw e
      }
    }
    builder.result
  }

  private val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      context.conf.get(AuthProviderParam.key).map(p => AuthProviderParam.key -> p).toMap.asJava,
      context.conf.getOrElse(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
    )

  val sizer: FileSize = new FileSize(fs, metadata)

  override val encoding: String = ParquetFileSystemStorage.Encoding

// TODO  ParquetFileSystemStorage.FileExtension

  override def getWriter(partition: Partition): FileSystemWriter =
    createWriter(partition, FileType.Written, metadata)

  override def getWriter(filter: Filter, threads: Int): FileSystemStorage.FileSystemUpdateWriter =
    new ParquetUpdateWriter(filter, threads)

  /**
   * Register a new file with this storage instance. The file must already be in a compatible format.
   *
   * @param file file to register
   * @return registered file
   */
  def register(file: URI): DataFile = {
    val reader = createReader(None, None)
    val partitions = new java.util.HashSet[Partition]()
    val filePath = WithClose(reader.read(file)) { iter =>
      if (!iter.hasNext) {
        throw new RuntimeException("Could not read any features from input file")
      }
      iter.foreach { sf =>
        partitions.add(Partition(metadata.schemes.map(_.getPartition(sf))))
      }
      FileSystemStorage.newFilePath(metadata.sft.getTypeName, FileType.Written, encoding)
    }
    if (partitions.size() != 1) {
      throw new IllegalArgumentException(s"File corresponds to multiple partitions: ${partitions.asScala.mkString(" AND ")}")
    }

    val partition = partitions.iterator().next()
    val destination = context.root.resolve(filePath)
    logger.debug(s"Copying $file to $destination")
    fs.copy(file, destination)

    val dataFile = metadata.createDataFile(filePath, partition)
    metadata.addFile(dataFile)
    dataFile
  }

  /**
   * Compact a partition - merge multiple data files into a single file.
   *
   * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
   * multiple threads or storage instances attempt to compact the same partition simultaneously.
   *
   * @param partition partition to compact, or all partitions
   * @param threads suggested threads to use for file system operations
   */
  def compact(partition: Partition, threads: Int = 1): Unit = {
    val files = metadata.getFiles(partition)
    val toCompact = sizer.targetSize match {
      case None => files
      case Some(t) =>
        files.filter { f =>
          val filePath = f.location() // DataFile.location() returns full URI
          if (this.sizer.fileIsSized(java.net.URI.create(filePath), t)) {
            logger.debug(s"Skipping compaction for file [$filePath] (already target size)")
            false
          } else {
            true
          }
        }
    }

    if (toCompact.isEmpty) {
      logger.debug("Skipping compaction - no files to compact")
    } else {
      logger.debug(s"Compacting data files: [${toCompact.map(_.location()).mkString(", ")}]")

      var written = 0L

      val reader = createReader(None, None)
      // tracks newly added files so we can register them atomically
      val fileTracker = new FileTracker(metadata)

      WithClose(createWriter(partition, FileType.Compacted, fileTracker)) { writer =>
        WithClose(FileSystemThreadedReader(reader, toCompact, threads)) { reader =>
          while (reader.hasNext) {
            val feature = reader.next()
            writer.write(feature)
            written += 1
          }
        }
      }

      logger.debug(s"Updating metadata with new files: [${fileTracker.getFiles().map(_.location()).mkString(", ")}]")
      metadata.replaceFiles(toCompact, fileTracker.getFiles())

      logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")
      val failures = ArrayBuffer.empty[String]
      toCompact.foreach { file =>
        val path = new URI(file.location())
        if (Try(fs.delete(path)).isFailure) {
          failures.append(file.location())
        }
      }

      if (failures.nonEmpty) {
        logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
      }

      logger.debug(s"Compacted $written records")
    }
  }

  override def close(): Unit = CloseWithLogging(metadata, fs)

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    // readSchema has all the fields needed for filtering and return
    val readSchema = ReadSchema(metadata.sft, filter, transform)
    val readSft = readSchema.read.getOrElse(metadata.sft)
    val ReadFilter(fc, residualFilter) = ReadFilter(readSft, filter)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val gtFilter = residualFilter.map(FastFilterFactory.optimize(readSft, _))
    val visFilter = VisibilityUtils.visible(authProvider)

    logger.debug(
      s"    Parquet filter: ${parquetFilter match { case f: FilterPredicateCompat => f.getFilterPredicate; case f => f }} " +
        s"and modified gt filter: ${gtFilter.getOrElse(Filter.INCLUDE)}")

    new ParquetFileSystemReader(fs, context, metadata.sft, readSchema.read, parquetFilter, gtFilter, visFilter, readSchema.transform)
  }

  /**
   * Create a writer for the given file
   *
   * @param file file to write to
   * @param partition partition being written to
   * @param observer observer to report stats on the data written
   * @return
   */
  private def createWriter(file: URI, partition: Partition, observer: FileSystemObserver): FileSystemWriter = {
    val conf = context.conf ++ Map(SimpleFeatureParquetSchema.PartitionKey -> partition.toString)

    val observers =
      if (FileValidationEnabled.toBoolean.get) {
        CompositeObserver(Seq(observer, FileValidationObserver(file)))
      } else {
        observer
      }
    new ParquetFileSystemWriter(fs, conf, metadata.sft, file, observers)
  }

  /**
   * Create a new writer
   *
   * @param partition partition being written to
   * @param fileType file type
   * @param metadata metadata to track added files
   * @return
   */
  private def createWriter(
    partition: Partition,
    fileType: FileType,
    metadata: StorageMetadata): FileSystemWriter = {

    def newWriter(): FileSystemWriter = {
      val file = FileSystemStorage.newFilePath(metadata.sft.getTypeName, fileType, encoding)
      val path = context.root.resolve(file)
      val updateObserver = new MetadataObserver(metadata, file, partition)
      val observer = if (observers.isEmpty) { updateObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(updateObserver))
      }
      createWriter(path, partition, observer)
    }

    sizer.targetSize match {
      case None => newWriter()
      case Some(s) => new ChunkedFileSystemWriter(Iterator.continually(newWriter()), sizer.estimator(s))
    }
  }

  /**
   * Update writer implementation
   *
   * @param filter update filter
   * @param threads query threads
   */
  private class ParquetUpdateWriter(filter: Filter, threads: Int) extends FileSystemUpdateWriter {

    private val reader = getReader(new Query(metadata.sft.getTypeName, filter), threads)

    // TODO limit number of open writers
    private val appenders = Caffeine.newBuilder().build(
      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
        override def load(key: Set[PartitionKey]): FileSystemWriter =
          createWriter(Partition(key), FileType.Written, metadata)
      }
    )
    private val modifiers = Caffeine.newBuilder().build(
      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
        override def load(key: Set[PartitionKey]): FileSystemWriter =
          createWriter(Partition(key), FileType.Modified, metadata)
      }
    )
    private val deleters = Caffeine.newBuilder().build(
      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
        override def load(key: Set[PartitionKey]): FileSystemWriter =
          createWriter(Partition(key), FileType.Deleted, metadata)
      }
    )

    private var feature: SimpleFeature = _
    private var partition: Set[PartitionKey] = _

    /**
     * Writes a modification to the last feature returned by `next`
     */
    override def write(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
      }
      val update = metadata.schemes.map(_.getPartition(feature))
      if (update == partition) {
        modifiers.get(update).write(feature)
      } else {
        // add a delete marker in the old partition, and an append in the new one, since we only track updates per-partition
        deleters.get(partition).write(feature)
        appenders.get(update).write(feature)
      }
      feature = null
    }

    /**
     * Deletes the last feature returned by `next`
     */
    override def remove(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'remove'")
      }
      deleters.get(partition).write(feature)
      feature = null
    }

    override def hasNext: Boolean = reader.hasNext

    override def next(): SimpleFeature = {
      feature = reader.next() // note: our reader returns a mutable copy of the feature
      partition = metadata.schemes.map(_.getPartition(feature))
      feature
    }

    override def flush(): Unit =
      FlushQuietly.raise(appenders.asMap().values().asScala.toSeq ++ modifiers.asMap().values().asScala ++ deleters.asMap().values().asScala)

    override def close(): Unit =
      CloseQuietly.raise(Seq(reader) ++ appenders.asMap().values().asScala ++ modifiers.asMap().values().asScala ++ deleters.asMap().values().asScala)
  }
}

object ParquetFileSystemStorage extends LazyLogging {

  val Encoding = "parquet"
  val FileExtension = "parquet"

  val ParquetCompressionOpt = "parquet.compression"

  /**
   * Writes files up to a given size, then starts a new file
   *
   * @param writers iterator of files to write
   * @param estimator target file size estimator
   */
  // noinspection ScalaWeakerAccess
  class ChunkedFileSystemWriter(writers: Iterator[FileSystemWriter], estimator: UpdatingFileSizeEstimator)
    extends FileSystemWriter {

    private var totalCount = 0L // total number of features written across all chunks
    private var totalBytes = 0L // sum size of all finished chunks
    private var remaining = estimator.estimate(0L)
    private var writer: FileSystemWriter = _

    override def write(feature: SimpleFeature): Unit = {
      if (writer == null) {
        writer = writers.next()
      }
      writer.write(feature)
      totalCount += 1
      remaining -= 1
      if (remaining == 0) {
        val dataSize = writer.size
        if (estimator.done(dataSize)) {
          writer.close()
          totalBytes += writer.size // re-calculate now that writer is closed, so we get the final, accurate size
          writer = null
          // adjust our estimate to account for the actual bytes written
          estimator.update(totalBytes, totalCount)
          remaining = estimator.estimate(0L)
        } else {
          remaining = math.max(100L, estimator.estimate(dataSize))
        }
      }
    }

    override def size: Long = totalBytes + Option(writer).fold(0L)(_.size)

    override def flush(): Unit = if (writer != null) { writer.flush() }

    override def close(): Unit = {
      if (writer != null) {
        writer.close()
      }
      estimator.close()
    }
  }


  /**
   * Observer to add a file to the metadata upon closing
   *
   * @param metadata metadata
   * @param path file path
   * @param partition file partition
   */
  private class MetadataObserver(metadata: StorageMetadata, path: String, partition: Partition)
    extends FileSystemObserver with LazyLogging {

    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {
      // Create DataFile by reading the file footer (fast operation)
      val dataFile = metadata.createDataFile(path, partition)
      logger.debug(s"Adding new metadata file: $path")
      metadata.addFile(dataFile)
    }
  }

  /**
   * Builds up attribute-level bounds
   *
   * @param i attribute index
   * @param lexicoder lexicoder for the attribute type
   */
  private case class ColumnBoundsBuilder(i: Int, lexicoder: TypeEncoder[AnyRef, String]) {

    private var lower: String = _
    private var upper: String = _

    def apply(feature: SimpleFeature): Unit = {
      val value = feature.getAttribute(i)
      if (value != null) {
        val encoded = lexicoder.encode(value)
        if (lower == null) {
          lower = encoded
          upper = encoded
        } else if (lower > encoded) {
          lower = encoded
        } else if (upper < encoded) {
          upper = encoded
        }
      }
    }

    def build(): Option[ColumnBounds] = if (lower == null) { None } else { Some(ColumnBounds(i, lower, upper)) }
  }

  /**
   * Can be used with a MetadataObserver to return data files instead of writing them directly to the metadata
   *
   * @param parent parent metadata to delegate createDataFile to
   */
  private class FileTracker(parent: StorageMetadata) extends StorageMetadata {

    import scala.collection.JavaConverters._

    private val files = new CopyOnWriteArrayList[DataFile]()

    override def sft: SimpleFeatureType = parent.sft
    override def schemes: Set[PartitionScheme] = parent.schemes
    override def `type`: String = "memory"

    override def createDataFile(filePath: String, partition: Partition): DataFile = parent.createDataFile(filePath, partition)
    override def addFiles(files: Seq[DataFile]): Unit = this.files.addAll(files.asJava)
    override def removeFile(file: DataFile): Unit = throw new UnsupportedOperationException()
    override def replaceFiles(existing: Seq[DataFile], replacements: Seq[DataFile]): Unit =
      throw new UnsupportedOperationException()
    override def getFiles(): Seq[DataFile] = files.asScala.toSeq
    override def getFiles(partition: Partition): Seq[DataFile] = throw new UnsupportedOperationException()
    override def getFiles(filter: Filter): Seq[DataFile] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }

  /**
   * Validate a file by reading it back
   *
   * @param file file to validate
   */
  case class FileValidationObserver(file: URI) extends FileSystemObserver {
    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {
      try {
        WithClose(ParquetReader.builder(new GroupReadSupport(), new Path(file)).build()) { reader =>
          var record = reader.read()
          while (record != null) {
            // Process the record
            record = reader.read()
          }
          logger.trace(s"$file is a valid Parquet file")
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"File appears to be corrupted: $file", e)
      }
    }
  }
}
