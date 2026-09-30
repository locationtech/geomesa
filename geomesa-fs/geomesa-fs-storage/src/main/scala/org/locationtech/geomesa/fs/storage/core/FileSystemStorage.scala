/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import io.micrometer.core.instrument.Tags
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.hadoop.fs.Path
import org.apache.iceberg._
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.types.{Conversions, Types}
import org.apache.iceberg.util.LocationUtil
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.FileBoundsParser.StringParser
import org.locationtech.geomesa.fs.storage.core.iceberg._
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.parquet.io.{ParquetFileSystemReader, ParquetFileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.utils.FileScan.FluentScan
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.fs.storage.core.utils.{FileScan, FileSize, MultiPartitionWriter}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.impl.MinMax.MinMaxDefaults
import org.locationtech.geomesa.index.utils.SortingSimpleFeatureIterator
import org.locationtech.geomesa.metrics.micrometer.utils.TagUtils
import org.locationtech.geomesa.security.{AuthProviderParam, AuthUtils, AuthorizationsProvider, AuthsParam, SecurityUtils, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, WithClose}
import org.locationtech.jts.geom.Geometry

import java.io.{Closeable, Flushable}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Locale, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Persists simple features to a file system and provides query access
 *
 * @param table iceberg table
 * @param schemes partition scheme
 * @param schema data file schema
 * @param conf configuration
 */
case class FileSystemStorage(
    table: Table,
    schemes: Seq[PartitionScheme],
    schema: SimpleFeatureIcebergSchema,
    conf: Map[String, String],
  ) extends Closeable with StrictLogging {

  import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileOps.{AttributeBoundsFileOps, BoundsFileOps, CountFileOps}
  import org.locationtech.geomesa.fs.storage.core.FileSystemStorage._
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  import scala.collection.JavaConverters._

  val sft: SimpleFeatureType = SimpleFeatureTypes.immutable(schema.sft)
  val sizer: FileSize = new FileSize(table)
  val metadata: DataFiles = new DataFiles()

  // common metrics tags for this storage instance
  val tags: Tags =
    Tags.of("store", getClass.getSimpleName.toLowerCase(Locale.US), "catalog", table.name()).and(TagUtils.typeNameTag(sft.getTypeName))

  protected val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      conf.get(AuthProviderParam.key).map(p => AuthProviderParam.key -> p).toMap.asJava,
      conf.getOrElse(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
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
    if (FileValidationEnabled.toBoolean.get) {
      builder += FileValidationObserverFactory
    }
    builder.result
  }

  private lazy val metricsConfig: MetricsConfig = MetricsConfig.forTable(table)

  private lazy val baseWritePath =
    Option(table.properties().get(TableProperties.WRITE_DATA_LOCATION))
      .map(LocationUtil.stripTrailingSlash)
      .getOrElse(s"${LocationUtil.stripTrailingSlash(table.location())}/data")

  /**
   * Get a reader for all relevant partitions
   *
   * @param query query
   * @param threads suggested threads used for reading data files
   * @return reader
   */
  def getReader(query: Query, threads: Int): CloseableIterator[SimpleFeature] = getReader(query, threads, forUpdate = false)

  /**
   * Gets a reader
   *
   * @param query query
   * @param threads read threads
   * @param forUpdate include row and file data required for positional deletes
   * @return
   */
  private[core] def getReader(query: Query, threads: Int, forUpdate: Boolean): CloseableIterator[SimpleFeature] = {
    val configured = QueryRunner.configureQuery(sft, query)
    val filter = Option(configured.getFilter).getOrElse(Filter.INCLUDE)
    val icebergFilter = IcebergFilterConverter(schema, schemes, filter)
    val visFilter = VisibilityUtils.visible(authProvider)
    val transform = configured.getHints.getTransform
    val includeFids = configured.getHints.isIncludeFid
    val sort = configured.getHints.getSortFields
    val max = configured.getHints.getMaxFeatures
    val readSchema = schema.read(transform, icebergFilter.remainder, icebergFilter.columns, includeFids, forUpdate)

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Push-down filter: ${icebergFilter.expression}")
    logger.debug(s"  Client-side filter: ${icebergFilter.remainder.fold("none")(ECQL.toCQL)}")
    logger.debug(s"  Transforms: ${transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }}}, with${if (includeFids) { "" } else { "out" }} FIDs")
    logger.debug(s"  Read schema: ${readSchema.schema.schema}")
    logger.debug(s"  Sort: ${sort.fold("none") { fields => fields.map { case (f, rev) => s"$f ${if (rev) "descending" else ""}"}.mkString(", ")}}")
    logger.debug(s"  Max features: ${max.getOrElse("none")}")

    val remainingFilter = icebergFilter.remainder.map(FastFilterFactory.optimize(readSchema.schema.sft, _))
    val transformer = readSchema.transforms.map { case (tsft, transforms) => new TransformSimpleFeature(tsft, transforms) }

    val scan = new IcebergParquetScan(table, readSchema.schema, icebergFilter.expression, threads)
    try {
      val visible = scan.filter(visFilter.apply)
      val filtered = remainingFilter.fold(visible)(f => visible.filter(f.evaluate))
      val transformed = transformer.fold(filtered)(t => filtered.map(t.setFeature))
      // note - have to copy the features since sorting will not just be sequential access
      val sorted = sort.fold(transformed)(new SortingSimpleFeatureIterator(transformed.map(ScalaSimpleFeature.copy), _))
      val limited = max.fold(sorted)(m => sorted.take(m))
      limited
    } catch {
      case NonFatal(e) => CloseWithLogging(scan); throw e
    }
  }

  /**
   * Gets the count of matching records.
   *
   * Note: may not be an exact count, depending on the filter. For an exact count, run a normal scan and count the results
   *
   * @param filter filter
   * @param threads number of threads to use for any scans
   * @return
   */
  def getCount(filter: Filter, threads: Int): Long = fileOps(filter, threads, new CountFileOps())

  /**
   * Gets the spatial bounds of matching records.
   *
   * Note: does not always account for deleted records
   *
   * @param filter filter
   * @return
   */
  def getBounds(filter: Filter, threads: Int): ReferencedEnvelope = fileOps(filter, threads, new BoundsFileOps(schema))

  /**
   * Gets the attribute bounds (min/max) of matching records.
   *
   * Note: does not always account for deleted records
   *
   * @param filter filter
   * @return
   */
  def getBounds[T](attribute: String, filter: Filter, threads: Int): (T, T) = {
    require(sft.indexOf(attribute) != -1,
      s"Attribute '$attribute' does not exist in the schema: ${sft.getTypeName} ${SimpleFeatureTypes.encodeType(sft)}")
    fileOps(filter, threads, new AttributeBoundsFileOps(schema, attribute))
  }

  /**
   * Read data files with visibilities. For optimized cases, operate directly on files that are wholly visible to the user.
   * For other cases, we have to read the file and evaluate row-by-row.
   *
   * @param filter filter
   * @param threads read threads
   * @param ops operations to run
   */
  private def fileOps[T](filter: Filter, threads: Int, ops: FileOps[T]): T = {
    val remainingFiles = ArrayBuffer.empty[String]
    val visFieldId = schema.schema.findField(SimpleFeatureSchema.VisibilitiesField).fieldId()
    val visFilter = VisibilityUtils.check(authProvider)

    val query = QueryRunner.configureQuery(sft, new Query(sft.getTypeName, filter, ops.readAttributes: _*))

    metadata.files().includeFileStats().forFilter(query.getFilter).scan().foreach { task =>
      val file = task.file()
      val nullValueCount = file.nullValueCounts().get(visFieldId)
      if (nullValueCount != null && nullValueCount == file.valueCounts().get(visFieldId)) {
        // all nulls
        if (!ops.fileOp(file, task.deletes().asScala.toSeq)) {
          remainingFiles += file.location()
        }
      } else {
        val lowerBound = StringParser.apply(file.lowerBounds().get(visFieldId))
        if (lowerBound != null && lowerBound == StringParser.apply(file.upperBounds().get(visFieldId))) {
          // only one vis marking, we can evaluate it here
          if (visFilter.apply(lowerBound)) {
            if (!ops.fileOp(file, task.deletes().asScala.toSeq)) {
              remainingFiles += file.location()
            }
          }
        } else {
          remainingFiles += file.location()
        }
      }
    }

    if (remainingFiles.nonEmpty) {
      val icebergFilter = IcebergFilterConverter(schema, schemes, query.getFilter)
      val transform = query.getHints.getTransform
      val readSchema = schema.read(transform, icebergFilter.remainder, icebergFilter.columns, includeFids = false)
      val fileFilter: Option[String => Boolean] = Some(remainingFiles.contains)
      WithClose(new IcebergParquetScan(table, readSchema.schema, icebergFilter.expression, threads, fileFilter)) { scan =>
        val features = icebergFilter.remainder.fold[CloseableIterator[SimpleFeature]](scan)(f => scan.filter(f.evaluate))
        features.foreach { f =>
          if (visFilter.apply(SecurityUtils.getVisibility(f))) {
            ops.featureOp(f)
          }
        }
      }
    }

    ops.result
  }

  /**
   * Get an appending writer for a given partition. This method is thread-safe and can be called multiple times,
   * but a given feature can only be appended/modified in a single thread, otherwise the behavior is undefined.
   *
   * @param partition partitions
   * @return writer
   */
  def getWriter(partition: Partition): FileSystemWriter = {
    val conf = this.conf ++ Map(SimpleFeatureSchema.PartitionKey -> partition.toString)

    def newWriter(): FileSystemWriter = {
      val path = newFilePath()
      val tableObserver = new AddDataFileObserver(path, partition)
      val observer = if (observers.isEmpty) { tableObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(tableObserver))
      }
      ParquetFileSystemWriter(schema, conf, table.io(), path, observer)
    }

    sizer.targetSize match {
      case None => newWriter()
      case Some(s) => new ChunkedFileSystemWriter(Iterator.continually(newWriter()), sizer.estimator(s))
    }
  }

  /**
   * Get an appending writer that will write to multiple partitions, based on the features being written.
   * Note that this writer does not support the `size` method
   *
   * @return
   */
  // noinspection AccessorLikeMethodIsEmptyParen
  def getMultiPartitionWriter(): FileSystemWriter = new MultiPartitionWriter(this, conf.getWriterMaxOpenPartitions)

  /**
   * Gets a modifying writer. This method is thread-safe and can be called multiple times, but a given feature
   * can only be appended/modified in a single thread, otherwise the behavior is undefined. There is no guarantee
   * that any concurrent modifications will be reflected in the returned writer.
   *
   * @param filter the filter used to select features for modification
   * @param threads suggested threads used for reading data files
   * @return
   */
  def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    IcebergUpdateWriter(this, filter, threads, conf.getWriterMaxOpenPartitions)

  override def close(): Unit = CloseWithLogging(Option(table).collect { case c: Closeable => c })

  /**
   * Helper for accessing metadata on files and partitions
   */
  class DataFiles {

    private val schemesWithIndex = schemes.zipWithIndex

    /**
     * Gets files in this storage instance
     *
     * @return
     */
    def files(): FluentScan = FileScan(table, schema, schemes)

    /**
     * Gets all partitions in this storage instance
     *
     * @return
     */
    def partitions(): Seq[Partition] = files().scan().map(t => partition(t.file())).distinct

    /**
     * Gets all partitions in this storage instance
     *
     * @return
     */
    def partitions(filter: Filter): Seq[Partition] = files().forFilter(filter).scan().map(t => partition(t.file())).distinct

    /**
     * Register new files with this storage instance. The files must already be in a compatible format.
     *
     * @param files files to register
     * @return registered files
     */
    def register(files: Map[Partition, Seq[URI]]): Seq[DataFile] = {
      val dataFiles = files.toSeq.flatMap { case (partition, paths) =>
        paths.map { path =>
          val destination = newFilePath()
          logger.debug(s"Copying $path to $destination")
          val uri = URI.create(destination)
          WithClose(ObjectStore(uri.getScheme, conf))(_.copy(path, uri))
          toDataFile(destination, partition)
        }
      }

      val append = table.newAppend()
      dataFiles.foreach(append.appendFile)
      append.commit()

      dataFiles
    }

    /**
     * Register new files with this storage instance. The files must already be in a compatible format.
     *
     * @param files files to register
     * @return registered file
     */
    def register(files: Seq[URI]): Seq[DataFile] = {
      val partitioned = scala.collection.mutable.Map.empty[String, ArrayBuffer[URI]]
      WithClose(ObjectStore(files.head.getScheme, conf)) { fs =>
        files.foreach { file =>
          WithClose(ParquetFileReader.open(ParquetFileSystemReader.inputFile(fs, file))) { reader =>
            val partition = reader.getFileMetaData.getKeyValueMetaData.get(SimpleFeatureSchema.PartitionKey)
            if (partition == null) {
              throw new RuntimeException(s"Could not load partition key from Parquet footer for file: $file")
            }
            partitioned.getOrElseUpdate(partition, ArrayBuffer.empty) += file
          }
        }
      }

      register(partitioned.map { case (k, v) => Partition(k) -> v.toSeq }.toMap)
    }

    /**
     * Register a new file with this storage instance. The file must already be in a compatible format
     *
     * @param file file to register
     * @return registered file
     */
    def register(file: URI): DataFile = register(Seq(file)).head

    /**
     * Compact manifest files to improve query performance
     */
    def compactManifests(): Unit = table.rewriteManifests().clusterBy(f => partition(f).toString).commit()

    /**
     * Compact a partition - merge multiple data files into a single file.
     *
     * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
     * multiple threads or storage instances attempt to compact the same partition simultaneously.
     *
     * @param partition partition to compact
     */
    def compact(partition: Partition): Unit = {
      // TODO implement compaction
      throw new UnsupportedOperationException("Not implemented")
    }

    /**
     * Extract the partition from a data file
     *
     * @param file data file
     * @return
     */
    def partition(file: DataFile): Partition =
      Partition(schemesWithIndex.map { case (s, i) => s.getPartition(file.partition(), i) })

    def partition(partition: Partition): PartitionData = {
      val data = new PartitionData(table.spec().partitionType)
      var i = 0
      partition.values.foreach { value =>
        data.set(i, Conversions.fromPartitionString(data.getType(i), value.value))
        i += 1
      }
      data
    }
  }

  private[core] def newFilePath(ext: String = "parquet"): String =
    s"$baseWritePath/${FileSystemStorage.newFilePath(sft.getTypeName, ext)}"

  /**
   * Reads the parquet metadata for a path and creates a data file
   *
   * @param path file path
   * @param partition file partition
   * @return
   */
  private def toDataFile(path: String, partition: Partition): DataFile = {
    val inputFile = table.io().newInputFile(path)
    val metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig, null)
    DataFiles.builder(table.spec())
      .withFormat(FileFormat.PARQUET)
      .withPath(inputFile.location())
      .withFileSizeInBytes(inputFile.getLength)
      .withPartition(metadata.partition(partition))
      .withMetrics(metrics)
      // TODO withSort(f.sort)
      .build()
  }

  /**
   * Observer to add a file to the metadata upon closing
   *
   * @param path file path
   * @param partition file partition
   */
  private class AddDataFileObserver(path: String, partition: Partition) extends FileSystemObserver {
    private var empty = true
    override def apply(feature: SimpleFeature): Unit = { empty = false }
    override def flush(): Unit = {}
    override def close(): Unit = {
      if (empty) {
        logger.warn(s"Not adding empty data file: $path")
      } else {
        // TODO this is reading the file footer again, could we track this during write instead?
        logger.debug(s"Adding new data file: $path")
        val file = toDataFile(path, partition)
        val append = table.newAppend()
        append.appendFile(file)
        append.commit()
      }
    }
  }
}

object FileSystemStorage extends LazyLogging {

  val ParquetCompressionOpt   = "parquet.compression"
  // row group ("block") size of written parquet files, as bytes or a size such as `128MB`; parquet's default if unset
  val ParquetRowGroupSizeOpt  = "parquet.block.size"
  val WriterMaxOpenPartitions = "fs.writer.partitions.max.open"
  val WriterMaxOpenPartitionsDefault = 32

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

  private case object FileValidationObserverFactory extends FileSystemObserverFactory {
    override def init(storage: FileSystemStorage): Unit = {}
    override def apply(path: String): FileSystemObserver = FileValidationObserver(path)
    override def close(): Unit = {}
  }

  /**
   * Validate a file by reading it back
   *
   * @param file file to validate
   */
  case class FileValidationObserver(file: String) extends FileSystemObserver {
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
   * @return
   */
  def newFilePath(typeName: String, ext: String = "parquet"): String = {
    val filename = s"${ColumnName.encode(typeName).take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.$ext"
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
  trait FileSystemUpdateWriter extends CloseableIterator[SimpleFeature] with Flushable {

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
     * Reads a file
     *
     * @param file file, relative to the root path
     * @return
     */
    def read(file: URI): CloseableIterator[SimpleFeature]
  }

  /**
   * Trait for executing metadata ops on data files
   */
  private trait FileOps[T] {

    /**
     * Result of the operation
     *
     * @return
     */
    def result: T

    /**
     * Operation for whole files - return true to indicate the file was handled, false to read the file row-by-row
     *
     * @param file data file
     * @param deletes delete markers
     * @return
     */
    def fileOp(file: DataFile, deletes: Seq[DeleteFile]): Boolean

    /**
     * Operation for individual records from a file
     *
     * @param feature feature
     */
    def featureOp(feature: SimpleFeature): Unit

    /**
     * Any attributes that are needed to evaluate the featureOp
     *
     * @return
     */
    def readAttributes: Seq[String]
  }

  private object FileOps {

    /**
     * Counts records
     */
    class CountFileOps extends FileOps[Long] {

      private var count: Long = 0

      override def result: Long = count

      override def fileOp(file: DataFile, deletes: Seq[DeleteFile]): Boolean = {
        var fileCount = file.recordCount()
        deletes.foreach { d =>
          fileCount -= d.recordCount()
        }
        if (fileCount > 0) {
          count += fileCount
        }
        true
      }

      override def featureOp(feature: SimpleFeature): Unit = count += 1

      override def readAttributes: Seq[String] = Seq.empty
    }

    /**
     * Reads spatial bounds
     *
     * @param schema schema
     */
    class BoundsFileOps(schema: SimpleFeatureIcebergSchema) extends FileOps[ReferencedEnvelope] {

      private val envelope = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)

      private val geomAttribute = schema.sft.getGeometryDescriptor.getLocalName
      private val bboxField = schema.schema.findField(BoundingBoxField.groupName(ColumnName.encode(geomAttribute))).`type`().asStructType()
      private val (minFieldIds, maxFieldIds) =
        Seq(BoundingBoxField.XMin, BoundingBoxField.YMin, BoundingBoxField.XMax, BoundingBoxField.YMax)
          .map(f => bboxField.field(f).fieldId())
          .splitAt(2)

      override def result: ReferencedEnvelope = envelope

      override def fileOp(file: DataFile, deletes: Seq[DeleteFile]): Boolean = {
        val minBuffers = minFieldIds.map(file.lowerBounds().get)
        val maxBuffers = maxFieldIds.map(file.upperBounds().get)
        if (minBuffers.contains(null) || maxBuffers.contains(null)) {
          false
        } else {
          val Seq(xmin, ymin) = minBuffers.map(Conversions.fromByteBuffer[Float](Types.FloatType.get(), _))
          val Seq(xmax, ymax) = maxBuffers.map(Conversions.fromByteBuffer[Float](Types.FloatType.get(), _))
          envelope.expandToInclude(xmin, ymin)
          envelope.expandToInclude(xmax, ymax)
          true
        }
      }

      override def featureOp(feature: SimpleFeature): Unit = {
        val geom = feature.getAttribute(geomAttribute).asInstanceOf[Geometry]
        if (geom != null) {
          envelope.expandToInclude(geom.getEnvelopeInternal)
        }
      }

      override def readAttributes: Seq[String] = Seq(geomAttribute)
    }

    /**
     * Read attribute bounds (min/max)
     *
     * @param schema schema
     * @param attribute attribute name
     * @tparam T attribute type binding
     */
    class AttributeBoundsFileOps[T](schema: SimpleFeatureIcebergSchema, attribute: String) extends FileOps[(T, T)] {

      private val field = schema.schema.findField(ColumnName.encode(attribute))
      private val fieldId = field.fieldId()

      private val descriptor = schema.sft.getDescriptor(attribute)
      private val parser = FileBoundsParser[T](descriptor, field.`type`()).orNull
      private val defaults = MinMaxDefaults[T](descriptor.getType.getBinding)
      private var min = defaults.max
      private var max = defaults.min

      override def result: (T, T) = (min, max)

      override def fileOp(file: DataFile, deletes: Seq[DeleteFile]): Boolean = {
        if (parser == null) { false } else {
          val localMin = parser(file.lowerBounds().get(fieldId))
          val localMax = parser(file.upperBounds().get(fieldId))
          if (localMin == null || localMax == null) {
            false
          } else {
            min = defaults.min(min, localMin)
            max = defaults.max(max, localMax)
            true
          }
        }

      }

      override def featureOp(feature: SimpleFeature): Unit = {
        val value = feature.getAttribute(attribute).asInstanceOf[T]
        if (value != null) {
          min = defaults.min(min, value)
          max = defaults.max(max, value)
        }
      }

      override def readAttributes: Seq[String] = Seq(attribute)
    }
  }
}
