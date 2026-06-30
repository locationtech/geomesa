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
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergFilterConverter, IcebergParquetScan, RecordSimpleFeature, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.parquet.io.{ParquetFileSystemReader, ParquetFileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.schema.{ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.utils.FileScan.FluentScan
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.fs.storage.core.utils.{FileScan, FileSize}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.SortingSimpleFeatureIterator
import org.locationtech.geomesa.security.{AuthProviderParam, AuthUtils, AuthorizationsProvider, AuthsParam, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.Transform.{PropertyTransform, Transforms}
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, WithClose}

import java.io.{Closeable, Flushable}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Persists simple features to a file system and provides query access
 *
 * @param context file system context
 * @param table iceberg table
 * @param schemes partition scheme
 * @param schema data file schema
 */
case class FileSystemStorage(
    context: FileSystemContext,
    table: Table,
    schemes: Seq[PartitionScheme],
    schema: SimpleFeatureIcebergSchema,
  ) extends Closeable with StrictLogging {

  import org.locationtech.geomesa.fs.storage.core.FileSystemStorage._

  import scala.collection.JavaConverters._

  val sft: SimpleFeatureType = SimpleFeatureTypes.immutable(schema.sft)
  val sizer: FileSize = new FileSize(table)
  val metadata: DataFiles = new DataFiles()

  protected val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      context.conf.get(AuthProviderParam.key).map(p => AuthProviderParam.key -> p).toMap.asJava,
      context.conf.getOrElse(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
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

  private lazy val metricsConfig: MetricsConfig = MetricsConfig.forTable(table)

  /**
   * Get a reader for all relevant partitions
   *
   * @param query query
   * @param threads suggested threads used for reading data files
   * @return reader
   */
  def getReader(query: Query, threads: Int): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val configured = QueryRunner.configureQuery(sft, query)
    val filter = Option(configured.getFilter).getOrElse(Filter.INCLUDE)
    val icebergFilter = IcebergFilterConverter(sft, filter)
    val visFilter = VisibilityUtils.visible(authProvider)
    val transform = configured.getHints.getTransform
    val sort = configured.getHints.getSortFields
    val max = configured.getHints.getMaxFeatures
    // note: readSchema keeps the columns in the original order, not the transform order
    val readSchema = schema.read(transform.map(_._1), icebergFilter.columns)

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Push-down filter: $icebergFilter")
    logger.debug(s"  Client-side filter: ${icebergFilter.remainder.fold("none")(ECQL.toCQL)}")
    logger.debug(s"  Transforms: ${transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }}}")
    logger.debug(s"  Sort: ${sort.fold("none") { fields => fields.map { case (f, rev) => s"$f ${if (rev) "descending" else ""}"}.mkString(", ")}}")
    logger.debug(s"  Max features: ${max.getOrElse("none")}")

    // TODO investigate - this doesn't seem to be pruning row groups effectively
    val tableScan =
      table.newScan()
        .caseSensitive(false)
        .select(readSchema.schema.columns().asScala.map(_.name()).asJava) // exclude z2 cols even if there's no transform
        .filter(icebergFilter.expression)

    val scan = {
      val ff = RecordSimpleFeature(readSchema)
      new IcebergParquetScan(tableScan, threads).map(ff.apply)
    }
    try {
      val iter = scan.filter(visFilter.apply)
      // apply any client side filter
      val filtered =
        icebergFilter.remainder.map(FastFilterFactory.optimize(readSchema.sft, _)).fold(iter)(f => iter.filter(f.evaluate))
      // transform again as necessary to remove any cols needed for filtering, and/or to evaluate complex expressions
      val transformed = transform.fold(filtered) { case (tdefs, tsft) =>
        val transforms = Transforms(readSchema.sft, tdefs).toArray
        if (tsft == readSchema.sft && transforms.forall(_.isInstanceOf[PropertyTransform])) {
          // simple case where transform is handled by the iceberg scan
          filtered
        } else {
          // need to re-order cols, remove filtering-only cols, evaluate transform expressions, etc
          filtered.map(sf => new TransformSimpleFeature(tsft, transforms).setFeature(sf))
        }
      }
      // sort and limit
      val sorted = sort.fold(transformed)(new SortingSimpleFeatureIterator(transformed, _))
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
  def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    throw new UnsupportedOperationException() // TODO support updates/deletes

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
    val compression = Option(System.getProperty(ParquetCompressionOpt)).map(ParquetCompressionOpt -> _).toMap
    val conf = compression ++ context.conf ++ Map(SimpleFeatureSchema.PartitionKey -> partition.toString)
    val observers =
      if (FileValidationEnabled.toBoolean.get) {
        CompositeObserver(Seq(observer, FileValidationObserver(file)))
      } else {
        observer
      }
    new ParquetFileSystemWriter(sft, conf, table.io(), file, observers)
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
      val file = FileSystemStorage.newFilePath(sft.getTypeName)
      val path = context.root.resolve(file)
      val tableObserver = content match {
        case FileContent.DATA =>
          new AddDataFileObserver(path, partition)
        // TODO support deletes
        // case FileContent.POSITION_DELETES =>
        // case FileContent.EQUALITY_DELETES =>
        case _ => throw new UnsupportedOperationException(s"Unsupported file content: $content")
      }
      val observer = if (observers.isEmpty) { tableObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(tableObserver))
      }
      createWriter(path, partition, observer)
    }

    sizer.targetSize match {
      case None => newWriter()
      case Some(s) => new ChunkedFileSystemWriter(Iterator.continually(newWriter()), sizer.estimator(s))
    }
  }

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
    def files(): FluentScan = FileScan(table, sft, schemes)

    /**
     * Gets all partitions in this storage instance
     *
     * @return
     */
    def partitions(): Seq[Partition] = files().scan().map(partition).distinct

    /**
     * Gets all partitions in this storage instance
     *
     * @return
     */
    def partitions(filter: Filter): Seq[Partition] = files().forFilter(filter).scan().map(partition).distinct

    /**
     * Register new files with this storage instance. The files must already be in a compatible format.
     *
     * @param files files to register
     * @return registered files
     */
    def register(files: Map[Partition, Seq[URI]]): Seq[DataFile] = {
      val dataFiles = files.toSeq.flatMap { case (partition, paths) =>
        paths.map { path =>
          val filePath = FileSystemStorage.newFilePath(sft.getTypeName)
          val destination = context.root.resolve(filePath)
          logger.debug(s"Copying $path to $destination")
          WithClose(ObjectStore(context))(_.copy(path, destination))
          toDataFile(destination.toString, partition)
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
      WithClose(ObjectStore(context)) { fs =>
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
  }

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
      .withPartitionValues(partition.values.map(_.value).asJava)
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
  private class AddDataFileObserver(path: URI, partition: Partition) extends FileSystemObserver {
    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {
      // TODO this is reading the file footer again, could we track this during write instead?
      logger.debug(s"Adding new data file: $path")
      val file = toDataFile(path.toString, partition)
      val append = table.newAppend()
      append.appendFile(file)
      append.commit()
    }
  }
}

object FileSystemStorage extends LazyLogging {

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
   * @return
   */
  def newFilePath(typeName: String): String = {
    val filename = s"${ColumnName.encode(typeName).take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.parquet"
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
    def read(file: URI): CloseableIterator[SimpleFeature]
  }
}
