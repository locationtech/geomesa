/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.hadoop.fs.Path
import org.apache.iceberg._
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergFilterConverter, IcebergParquetScan, IcebergSchemaMapper, RecordSimpleFeature}
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.parquet.io.{IcebergOutputFile, ParquetFileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.{ColumnName, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.fs.storage.core.utils.FileSize
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.SortingSimpleFeatureIterator
import org.locationtech.geomesa.security.{AuthProviderParam, AuthUtils, AuthorizationsProvider, AuthsParam, VisibilityUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, WithClose}

import java.io.{Closeable, Flushable}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.control.NonFatal

/**
 * Persists simple features to a file system and provides query access. Storage implementations are fairly
 * lightweight, in that all state is captured in the metadata instance
 */
class FileSystemStorage(val table: Table, val mapper: IcebergSchemaMapper) extends Closeable with StrictLogging {

  import org.locationtech.geomesa.fs.storage.core.FileSystemStorage._

  import scala.collection.JavaConverters._

  val sft: SimpleFeatureType = SimpleFeatureTypes.immutable(mapper.sft)
  val partitions: Seq[PartitionScheme] = mapper.schemes
  val context: FileSystemContext = mapper.context
  val sizer: FileSize = new FileSize(table)
  val files: Files = new Files()

  private val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      mapper.context.conf.get(AuthProviderParam.key).map(p => AuthProviderParam.key -> p).toMap.asJava,
      mapper.context.conf.getOrElse(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
    )

  // don't require observers if we never write any data
  private lazy val observers = {
    val builder = Seq.newBuilder[FileSystemObserverFactory]
    sft.getObservers.foreach { c =>
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

  private lazy val metricsConfig = MetricsConfig.forTable(table)

  /**
   * Get a reader for all relevant partitions
   *
   * @param query query
   * @param threads suggested threads used for reading data files
   * @return reader
   */
  def getReader(query: Query, threads: Int): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val configured = QueryRunner.configureQuery(sft, query)
    val filter = Option(configured.getFilter).getOrElse(Filter.INCLUDE)
    val (icebergFilter, clientSideFilter) = IcebergFilterConverter(sft, filter)
    val visFilter = VisibilityUtils.visible(authProvider)
    val transform = configured.getHints.getTransform
    val sort = configured.getHints.getSortFields
    val max = configured.getHints.getMaxFeatures
    val readSft = transform.fold(sft)(_._2)

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Push-down filter: $icebergFilter")
    logger.debug(s"  Client-side filter: ${clientSideFilter.fold("non")(ECQL.toCQL)}")
    logger.debug(s"  Transforms: ${transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }}}")
    logger.debug(s"  Sort: ${sort.fold("none") { fields => fields.map { case (f, rev) => s"$f ${if (rev) "descending" else ""}"}.mkString(", ")}}")
    logger.debug(s"  Max features: ${max.getOrElse("none")}")

    val tableScan = table.newScan().filter(icebergFilter)

    // TODO this will work for simple projections but not for complex transforms involving functions and such
    // TODO this also won't work if a field is being filtered but not returned
    // TODO bbox cols? not sure if we need to return them if we're filtering on them or not
    transform.foreach { case (_, tsft) =>
      val cols = Seq.newBuilder[String]
      cols += SimpleFeatureParquetSchema.FeatureIdField
      // TODO consolidate schema classes, this is a little round-about
      if (mapper.parquetSchema.hasVisibilities) {
        cols += SimpleFeatureParquetSchema.VisibilitiesField
      }
      tsft.getAttributeDescriptors.asScala.foreach(d => cols += ColumnName(d.getLocalName))
      tableScan.select(cols.result().asJava)
    }
    val sfFactory = RecordSimpleFeature(mapper.parquetSchema) // TODO this doesn't include transforms

    val scan = new IcebergParquetScan(tableScan, threads)
    try {
      val iter = scan.map(sfFactory.apply).filter(visFilter.apply)
      val filtered = clientSideFilter.map(FastFilterFactory.optimize(readSft, _)).fold(iter)(f => iter.filter(f.evaluate))
      val sorted = sort.fold(filtered)(s => new SortingSimpleFeatureIterator(filtered, s))
      val limited = max.fold(sorted)(m => sorted.take(m))
      limited
    } catch {
      case NonFatal(e) => CloseWithLogging(scan); throw e
    }
  }

  /**
   * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
   * although this can result in multiple data files.
   *
   * @param partition partitions
   * @return writer
   */
  def getWriter(partition: Partition): FileSystemWriter = createWriter(partition, FileContent.DATA)

  /**
   * Gets a modifying writer. This method is thread-safe and can be called multiple times,
   * although if a feature is modified multiple times concurrently, the last update 'wins'.
   * There is no guarantee that any concurrent modifications will be reflected in the returned
   * writer.
   *
   * @param filter the filter used to select features for modification
   * @param threads suggested threads used for reading data files
   * @return
   */
  def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter = {
    throw new UnsupportedOperationException() // TODO
//    new FileSystemUpdateWriter(filter, threads)
  }

//  /**
//   * Register a new file with this storage instance. The file must already be in a compatible format.
//   *
//   * @param file file to register
//   * @return registered file
//   */
//  def register(file: URI): DataFile = {
//    val reader = createReader(None, None)
//    val partitions = new java.util.HashSet[Partition]()
//    val filePath = WithClose(reader.read(file)) { iter =>
//      if (!iter.hasNext) {
//        throw new RuntimeException("Could not read any features from input file")
//      }
//      iter.foreach { sf =>
//        partitions.add(Partition(metadata.schemes.map(_.getPartition(sf))))
//      }
//      FileSystemStorage.newFilePath(metadata.sft.getTypeName, FileContent.DATA, "parquet")
//    }
//    if (partitions.size() != 1) {
//      throw new IllegalArgumentException(s"File corresponds to multiple partitions: ${partitions.asScala.mkString(" AND ")}")
//    }
//
//    val partition = partitions.iterator().next()
//    val destination = context.root.resolve(filePath)
//    logger.debug(s"Copying $file to $destination")
//    fs.copy(file, destination)
//
//    val dataFile = metadata.createDataFile(filePath, partition)
//    metadata.addFile(dataFile)
//    dataFile
//  }

  /**
   * Compact a partition - merge multiple data files into a single file.
   *
   * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
   * multiple threads or storage instances attempt to compact the same partition simultaneously.
   *
   * @param partition partition to compact, or all partitions
   * @param threads suggested threads to use for file system operations
   */
//  def compact(partition: Partition, threads: Int = 1): Unit = {
//    val files = metadata.getFiles(partition)
//    val toCompact = sizer.targetSize match {
//      case None => files
//      case Some(t) =>
//        files.filter { f =>
//          val filePath = f.location() // DataFile.location() returns full URI
//          if (this.sizer.fileIsSized(java.net.URI.create(filePath), t)) {
//            logger.debug(s"Skipping compaction for file [$filePath] (already target size)")
//            false
//          } else {
//            true
//          }
//        }
//    }
//
//    if (toCompact.isEmpty) {
//      logger.debug("Skipping compaction - no files to compact")
//    } else {
//      logger.debug(s"Compacting data files: [${toCompact.map(_.location()).mkString(", ")}]")
//
//      var written = 0L
//
//      val reader = createReader(None, None)
//      // tracks newly added files so we can register them atomically
//      val fileTracker = new FileTracker(metadata)
//
//      WithClose(createWriter(partition, org.apache.iceberg.FileContent.DATA, fileTracker)) { writer =>
//        WithClose(FileSystemThreadedReader(reader, toCompact, threads)) { reader =>
//          while (reader.hasNext) {
//            val feature = reader.next()
//            writer.write(feature)
//            written += 1
//          }
//        }
//      }
//
//      logger.debug(s"Updating metadata with new files: [${fileTracker.getFiles().map(_.location()).mkString(", ")}]")
//      metadata.replaceFiles(toCompact, fileTracker.getFiles())
//
//      logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")
//      val failures = ArrayBuffer.empty[String]
//      toCompact.foreach { file =>
//        val path = new URI(file.location())
//        if (Try(fs.delete(path)).isFailure) {
//          failures.append(file.location())
//        }
//      }
//
//      if (failures.nonEmpty) {
//        logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
//      }
//
//      logger.debug(s"Compacted $written records")
//    }
//  }

  override def close(): Unit = CloseWithLogging(Option(table).collect { case c: Closeable => c })

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
    val out = new IcebergOutputFile(table.io().newOutputFile(file.toString))
    new ParquetFileSystemWriter(sft, conf, out, out.original.toInputFile.getLength, observers)
  }

  /**
   * Create a new writer
   *
   * @param partition partition being written to
   * @param content file type
   * @return
   */
  private def createWriter(partition: Partition, content: FileContent): FileSystemWriter = {
    def newWriter(): FileSystemWriter = {
      val file = FileSystemStorage.newFilePath(sft.getTypeName, content)
      val path = context.root.resolve(file)
      val updateObserver = new MetadataObserver(path, partition, content)
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

//  /**
//   * Update writer implementation
//   *
//   * @param filter update filter
//   * @param threads query threads
//   */
//  class FileSystemUpdateWriter(filter: Filter, threads: Int) extends Iterator[SimpleFeature] with Closeable with Flushable  {
//
//    private val reader = getReader(new Query(metadata.sft.getTypeName, filter), threads)
//
//    // TODO limit number of open writers
//    private val appenders = Caffeine.newBuilder().build(
//      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
//        override def load(key: Set[PartitionKey]): FileSystemWriter =
//          createWriter(Partition(key), org.apache.iceberg.FileContent.DATA, metadata)
//      }
//    )
//    // Legacy modification support - still uses DATA files with m_ prefix for backward compatibility
//    // TODO: Consider refactoring to use proper Iceberg update semantics (delete + rewrite)
//    private val modifiers = Caffeine.newBuilder().build(
//      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
//        override def load(key: Set[PartitionKey]): FileSystemWriter =
//          createWriter(Partition(key), org.apache.iceberg.FileContent.DATA, metadata)
//      }
//    )
//    // Delete files use Iceberg EQUALITY_DELETES content type
//    private val deleters = Caffeine.newBuilder().build(
//      new CacheLoader[Set[PartitionKey], FileSystemWriter]() {
//        override def load(key: Set[PartitionKey]): FileSystemWriter =
//          createWriter(Partition(key), org.apache.iceberg.FileContent.EQUALITY_DELETES, metadata)
//      }
//    )
//
//    private var feature: SimpleFeature = _
//    private var partition: Set[PartitionKey] = _
//
//    /**
//     * Writes a modification to the last feature returned by `next`
//     */
//    def write(): Unit = {
//      if (feature == null) {
//        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
//      }
//      val update = metadata.schemes.map(_.getPartition(feature))
//      if (update == partition) {
//        modifiers.get(update).write(feature)
//      } else {
//        // add a delete marker in the old partition, and an append in the new one, since we only track updates per-partition
//        deleters.get(partition).write(feature)
//        appenders.get(update).write(feature)
//      }
//      feature = null
//    }
//
//    /**
//     * Deletes the last feature returned by `next`
//     */
//    def remove(): Unit = {
//      if (feature == null) {
//        throw new IllegalArgumentException("Must call 'next' before calling 'remove'")
//      }
//      deleters.get(partition).write(feature)
//      feature = null
//    }
//
//    override def hasNext: Boolean = reader.hasNext
//
//    override def next(): SimpleFeature = {
//      feature = reader.next() // note: our reader returns a mutable copy of the feature
//      partition = metadata.schemes.map(_.getPartition(feature))
//      feature
//    }
//
//    override def flush(): Unit =
//      FlushQuietly.raise(appenders.asMap().values().asScala.toSeq ++ modifiers.asMap().values().asScala ++ deleters.asMap().values().asScala)
//
//    override def close(): Unit =
//      CloseQuietly.raise(Seq(reader) ++ appenders.asMap().values().asScala ++ modifiers.asMap().values().asScala ++ deleters.asMap().values().asScala)
//  }

  class Files {

    def partitions(): Seq[Partition] =
      WithClose(table.newScan().planFiles())(_.asScala.map(f => mapper.partition(f.file())).toSeq.distinct)

    def files(): Seq[DataFile] = WithClose(table.newScan().planFiles())(_.asScala.map(_.file()).toSeq)

    def files(partition: Partition): Seq[DataFile] =
      WithClose(table.newScan().filter(mapper.expression(partition)).planFiles())(_.asScala.map(_.file()).toSeq)
  }

  /**
   * Observer to add a file to the metadata upon closing
   *
   * @param path file path
   * @param partition file partition
   * @param content file content type (DATA, EQUALITY_DELETES, POSITION_DELETES)
   */
  private class MetadataObserver(path: URI, partition: Partition, content: FileContent)
      extends FileSystemObserver {

    private val partitionValues = mapper.partitionValues(partition)

    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {
      content match {
        case org.apache.iceberg.FileContent.DATA =>
          logger.debug(s"Adding new data file: $path")
          val inputFile = table.io().newInputFile(path.toString)
          // TODO this is reading the file footer again, could we track this during write intead?
          val metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig, null)
          val file =
            DataFiles.builder(table.spec())
              .withFormat(FileFormat.PARQUET)
              .withPath(inputFile.location())
              .withFileSizeInBytes(inputFile.getLength)
              .withPartitionValues(partitionValues.asJava)
              .withMetrics(metrics)
              // TODO withSort(f.sort)
              .build()
          val append = table.newAppend()
          append.appendFile(file)
          append.commit()

          // TODO
          //        case org.apache.iceberg.FileContent.POSITION_DELETES =>
          //        case org.apache.iceberg.FileContent.EQUALITY_DELETES =>
        case _ => throw new UnsupportedOperationException(s"Unsupported file content: $content")
      }
    }
  }
}

object FileSystemStorage extends LazyLogging {

  val ParquetCompressionOpt = "parquet.compression"

  private final val SafeNameRegex = "[^a-zA-Z0-9_-]+".r

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

//  /**
//   * Builds up attribute-level bounds
//   *
//   * @param i attribute index
//   * @param lexicoder lexicoder for the attribute type
//   */
//  private case class ColumnBoundsBuilder(i: Int, lexicoder: TypeEncoder[AnyRef, String]) {
//
//    private var lower: String = _
//    private var upper: String = _
//
//    def apply(feature: SimpleFeature): Unit = {
//      val value = feature.getAttribute(i)
//      if (value != null) {
//        val encoded = lexicoder.encode(value)
//        if (lower == null) {
//          lower = encoded
//          upper = encoded
//        } else if (lower > encoded) {
//          lower = encoded
//        } else if (upper < encoded) {
//          upper = encoded
//        }
//      }
//    }
//
//    def build(): Option[ColumnBounds] = if (lower == null) { None } else { Some(ColumnBounds(i, lower, upper)) }
//  }
//
//  /**
//   * Can be used with a MetadataObserver to return data files instead of writing them directly to the metadata
//   *
//   * @param parent parent metadata to delegate createDataFile to
//   */
//  private class FileTracker(parent: StorageMetadata) extends StorageMetadata {
//
//    import scala.collection.JavaConverters._
//
//    private val files = new CopyOnWriteArrayList[DataFile]()
//
//    override def sft: SimpleFeatureType = parent.sft
//    override def schemes: Set[PartitionScheme] = parent.schemes
//    override def `type`: String = "memory"
//
//    override def createDataFile(
//      filePath: String,
//      partition: Partition,
//      content: org.apache.iceberg.FileContent = org.apache.iceberg.FileContent.DATA): DataFile =
//      parent.createDataFile(filePath, partition, content)
//    override def addFiles(files: Seq[DataFile]): Unit = this.files.addAll(files.asJava)
//    override def removeFile(file: DataFile): Unit = throw new UnsupportedOperationException()
//    override def replaceFiles(existing: Seq[DataFile], replacements: Seq[DataFile]): Unit =
//      throw new UnsupportedOperationException()
//    override def getFiles(): Seq[DataFile] = files.asScala.toSeq
//    override def getFiles(partition: Partition): Seq[DataFile] = throw new UnsupportedOperationException()
//    override def getFiles(filter: Filter): Seq[DataFile] = throw new UnsupportedOperationException()
//    override def close(): Unit = {}
//  }

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

  /**
   * Get the path for a new data file, using Iceberg FileContent semantics
   *
   * @param typeName simple feature type name
   * @param content Iceberg file content type (DATA, POSITION_DELETES, EQUALITY_DELETES)
   * @return
   */
  def newFilePath(typeName: String, content: FileContent): String = {
    val prefix = content match {
      case org.apache.iceberg.FileContent.DATA => ""
      case org.apache.iceberg.FileContent.POSITION_DELETES => "xp_"
      case org.apache.iceberg.FileContent.EQUALITY_DELETES => "xe_"
      case _ => throw new UnsupportedOperationException(s"Unsupported file content: $content")
    }
    val filename =
      s"$prefix${SafeNameRegex.replaceAllIn(typeName, "-").take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.parquet"
    // partitioning logic taken from Apache Iceberg: https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout
    val hash = {
      val bytes = filename.getBytes(StandardCharsets.UTF_8)
      val hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
      // Integer#toBinaryString excludes leading zeros, which we want to preserve
      Integer.toBinaryString(hash | Integer.MIN_VALUE)
    }
    s"${hash.substring(0, 4)}/${hash.substring(4, 8)}/${hash.substring(8, 12)}/${hash.substring(12, 20)}/$filename"
  }

  /**
   * Append writer
   */
  trait FileSystemWriter extends Closeable with Flushable {

    /**
      * Write a feature
      *
      * @param feature feature
      */
    def write(feature: SimpleFeature): Unit

    /**
     * Gets the size of the data written so far, in bytes. May not be accurate until the writer is
     * closed, due to buffering, etc
     *
     * @return
     */
    def size: Long
  }

  /**
   * Update writer
   *
   */
  trait FileSystemUpdateWriter extends Iterator[SimpleFeature] with Closeable with Flushable {

    /**
     * Writes a modification to the last feature returned by `next`
     */
    def write(): Unit

    /**
     * Deletes the last feature returned by `next`
     */
    def remove(): Unit
  }

  /**
   * Reader trait
   */
  trait FileSystemPathReader {

    /**
     * Root path
     *
     * @return
     */
    def root: URI

    /**
     * Reads a file
     *
     * @param file file, relative to the root path
     * @return
     */
    def read(file: URI): Iterator[SimpleFeature] with Closeable
  }
}
