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
import org.calrissian.mango.types.TypeEncoder
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileType.FileType
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage._
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.metadata.StorageMetadata
import org.locationtech.geomesa.fs.storage.core.metadata.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.core.metadata.StorageMetadata.{AttributeBounds, Partition, PartitionKey, SpatialBounds, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.partitions.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.fs.storage.core.utils.{FileSize, FileSystemThreadedReader}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly, WithClose}
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.io.{Closeable, Flushable}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Persists simple features to a file system and provides query access. Storage implementations are fairly
 * lightweight, in that all state is captured in the metadata instance
 *
 * @param context handle to the file context, root path and configuration
 * @param metadata metadata on files for this instance
 * @param encoding file encoding used by this instance
 */
abstract class FileSystemStorage(val context: FileSystemContext, val metadata: StorageMetadata, val encoding: String)
    extends Closeable with StrictLogging {

  import scala.collection.JavaConverters._

  require(context.root.toString.endsWith("/"), "Root path must end with a '/'")

  private val fileSize = new FileSize(context, metadata)

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
        observer.init(context.conf, context.root, metadata.sft)
      } catch {
        case NonFatal(e) => CloseQuietly(builder.result).foreach(e.addSuppressed); throw e
      }
    }
    builder.result
  }

  /**
    * Get a reader for all relevant partitions
    *
    * @param query query
    * @param threads suggested threads used for reading data files
    * @return reader
    */
  def getReader(query: Query, threads: Int = 1): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val (filter, transform) = {
      val configured = QueryRunner.configureQuery(metadata.sft, query)
      val filter = Option(configured.getFilter).getOrElse(Filter.INCLUDE)
      val transform = configured.getHints.getTransform
      logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter)}")
      logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
      logger.debug(s"  Transforms: " + transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }})
      (filter, transform)
    }

    val files = metadata.getFiles(filter)
    logger.debug(s"  Threading the read of ${files.size} files with $threads reader threads")
    logger.whenTraceEnabled(files.foreach(f => logger.trace(s"    $f")))

    if (files.isEmpty) {
      CloseableIterator.empty
    } else {
      val reader = createReader(Option(filter).filterNot(_ == Filter.INCLUDE), transform)
      FileSystemThreadedReader(reader, files, threads)
    }
  }

  /**
    * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
    * although this can result in multiple data files.
    *
    * @param partition partitions
    * @return writer
    */
  def getWriter(partition: Partition): FileSystemWriter = createWriter(partition, StorageFileAction.Append)

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
  def getWriter(filter: Filter, threads: Int = 1): FileSystemUpdateWriter =
    new FileSystemUpdateWriter(metadata.schemes, getReader(new Query(metadata.sft.getTypeName, filter), threads), createWriter)

  /**
   * Register a new file with this storage instance. The file must already be in a compatible format.
   *
   * @param file file to register
   * @return registered file
   */
  def register(file: URI): StorageFile = {
    val reader = createReader(None, None)
    val partitions = new java.util.HashSet[Partition]()
    val storageFile = WithClose(new StorageFileObserver(metadata.sft)) { observer =>
      WithClose(reader.read(file)) { iter =>
        if (!iter.hasNext) {
          throw new RuntimeException("Could not read any features from input file")
        }
        iter.foreach { sf =>
          partitions.add(Partition(metadata.schemes.map(_.getPartition(sf))))
          observer(sf)
        }
      }
      val filePath = FileSystemStorage.newFilePath(metadata.sft.getTypeName, FileType.Written, encoding)
      observer.file(filePath, partitions.iterator().next(), StorageFileAction.Append)
    }
    if (partitions.size() != 1) {
      throw new IllegalArgumentException(s"File corresponds to multiple partitions: ${partitions.asScala.mkString(" AND ")}")
    }

    val destination = context.root.resolve(storageFile.file)
    logger.debug(s"Copying $file to $destination")
    context.fs.copy(file, destination)
    metadata.addFile(storageFile)
    storageFile
  }

  /**
   * Compact a partition - merge multiple data files into a single file.
   *
   * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
   * multiple threads or storage instances attempt to compact the same partition simultaneously.
   *
   * @param partition partition to compact, or all partitions
   * @param fileSize approximate target size of files, in bytes
   * @param threads suggested threads to use for file system operations
   */
  def compact(partition: Partition, fileSize: Option[Long] = None, threads: Int = 1): Unit = {
    val target = fileSize.orElse(this.fileSize.targetSize)
    val files = metadata.getFiles(partition)
    val toCompact = target match {
      case None => files
      case Some(t) =>
        files.filter { f =>
          if (this.fileSize.fileIsSized(context.root.resolve(f.file), t)) {
            logger.debug(s"Skipping compaction for file [${f.file}] (already target size)")
            false
          } else {
            true
          }
        }
    }

    if (toCompact.isEmpty) {
      logger.debug("Skipping compaction - no files to compact")
    } else {
      logger.debug(s"Compacting data files: [${toCompact.map(_.file).mkString(", ")}]")

      var written = 0L

      val reader = createReader(None, None)
      // tracks newly added files so we can register them atomically
      val fileTracker = new FileTracker(metadata.sft, metadata.schemes)

      WithClose(createWriter(partition, StorageFileAction.Append, Some(FileType.Compacted), target, fileTracker)) { writer =>
        WithClose(FileSystemThreadedReader(reader, toCompact, threads)) { reader =>
          while (reader.hasNext) {
            val feature = reader.next()
            writer.write(feature)
            written += 1
          }
        }
      }

      logger.debug(s"Updating metadata with new files: [${fileTracker.getFiles().map(_.file).mkString(", ")}]")
      metadata.replaceFiles(toCompact, fileTracker.getFiles())

      logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")
      val failures = ArrayBuffer.empty[String]
      toCompact.foreach { file =>
        val path = context.root.resolve(file.file)
        if (Try(context.fs.delete(path)).isFailure) {
          failures.append(file.file)
        }
        // TODO
//        PathCache.invalidate(context.fs, path)
      }

      if (failures.nonEmpty) {
        logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
      }

      logger.debug(s"Compacted $written records")
    }
  }

  override def close(): Unit = metadata.close()

  /**
   * Create a writer for the given file
   *
   * @param file file to write to
   * @param partition partition being written to
   * @param observer observer to report stats on the data written
   * @return
   */
  protected def createWriter(file: URI, partition: Partition, observer: FileSystemObserver): FileSystemWriter

  /**
   * Create a path reader with the given filter and transform
   *
   * @param filter filter, if any
   * @param transform transform
   * @return
   */
  protected def createReader(filter: Option[Filter], transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader

  /**
   * Create a new writer
   *
   * @param partition partition being written to
   * @param action write type
   * @param fileType file type
   * @param targetFileSize target file size
   * @param metadata metata to track added files
   * @return
   */
  private def createWriter(
    partition: Partition,
    action: StorageFileAction,
    fileType: Option[FileType] = None,
    targetFileSize: Option[Long] = None,
    metadata: StorageMetadata = this.metadata): FileSystemWriter = {

    val ft = fileType.getOrElse {
      action match {
        case StorageFileAction.Append => FileType.Written
        case StorageFileAction.Modify => FileType.Modified
        case StorageFileAction.Delete => FileType.Deleted
      }
    }
    def pathAndWriter: (URI, FileSystemWriter) = {
      val file = FileSystemStorage.newFilePath(metadata.sft.getTypeName, ft, encoding)
      val path = context.root.resolve(file)
      val updateObserver = new MetadataObserver(metadata, file, partition, action)
      val observer = if (observers.isEmpty) { updateObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(updateObserver))
      }
      (path, createWriter(path, partition, observer))
    }

    targetFileSize.orElse(fileSize.targetSize) match {
      case None => pathAndWriter._2
      case Some(s) => new ChunkedFileSystemWriter(context.fs, Iterator.continually(pathAndWriter), fileSize.estimator(s))
    }
  }
}

object FileSystemStorage {

  private final val SafeNameRegex = "[^a-zA-Z0-9_-]+".r

  /**
   * Get the path for a new data file
   *
   * @param ext file extension
   * @param fileType file type
   * @return
   */
  def newFilePath(typeName: String, fileType: FileType.FileType, ext: String): String = {
    val filename =
      s"${fileType}_${SafeNameRegex.replaceAllIn(typeName, "-").take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.$ext"
    // partitioning logic taken from Apache Iceberg: https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout
    val hash = {
      val bytes = filename.getBytes(StandardCharsets.UTF_8)
      val hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
      // Integer#toBinaryString excludes leading zeros, which we want to preserve
      Integer.toBinaryString(hash | Integer.MIN_VALUE)
    }
    s"${hash.substring(0, 4)}/${hash.substring(4, 8)}/${hash.substring(8, 12)}/${hash.substring(12, 20)}/$filename"
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("w")
    val Compacted: Value = Value("c")
    val Modified : Value = Value("m")
    val Deleted  : Value = Value("d")
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
  }

  /**
   * Writes files up to a given size, then starts a new file
   *
   * @param fs file system
   * @param writers iterator of files to write
   * @param estimator target file size estimator
   */
  private class ChunkedFileSystemWriter(
      fs: ObjectStore,
      writers: Iterator[(URI, FileSystemWriter)],
      estimator: UpdatingFileSizeEstimator
    ) extends FileSystemWriter {

    private var count = 0L // number of features written
    private var total = 0L // sum size of all finished chunks
    private var remaining = estimator.estimate(0L)

    private var path: URI = _
    private var writer: FileSystemWriter = _

    override def write(feature: SimpleFeature): Unit = {
      if (writer == null) {
        val (path, writer) = writers.next()
        this.path = path
        this.writer = writer
      }
      writer.write(feature)
      count += 1
      remaining -= 1
      if (remaining == 0) {
        writer.close()
        writer = null
        // adjust our estimate to account for the actual bytes written
        total += fs.size(path)
        estimator.update(total, count)
        remaining = estimator.estimate(0L)
      }
    }

    override def flush(): Unit = if (writer != null) { writer.flush() }

    override def close(): Unit = {
      if (writer != null) {
        writer.close()
      }
      estimator.close()
    }
  }

  /**
   * Update writer implementation
   *
   * @param reader reader for features to update
   */
  class FileSystemUpdateWriter(
      schemes: Set[PartitionScheme],
      reader: CloseableFeatureIterator,
      newWriter: (Partition, StorageFileAction) => FileSystemWriter
    ) extends Iterator[SimpleFeature] with Closeable with Flushable {

    // TODO limit number of open writers

    private val appenders = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]
    private val modifiers = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]
    private val deleters = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]

    private var feature: SimpleFeature = _
    private var partition: Set[PartitionKey] = _

    /**
     * Writes a modification to the last feature returned by `next`
     */
    def write(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
      }
      val update = schemes.map(_.getPartition(feature))
      if (update == partition) {
        modifiers.getOrElseUpdate(update, newWriter(Partition(update), StorageFileAction.Modify)).write(feature)
      } else {
        // add a delete marker in the old partition, and an append in the new one, since we only track updates per-partition
        deleters.getOrElseUpdate(partition, newWriter(Partition(partition), StorageFileAction.Delete)).write(feature)
        appenders.getOrElseUpdate(update, newWriter(Partition(update), StorageFileAction.Append)).write(feature)
      }
      feature = null
    }

    /**
     * Deletes the last feature returned by `next`
     */
    def remove(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'remove'")
      }
      deleters.getOrElseUpdate(partition, newWriter(Partition(partition), StorageFileAction.Delete)).write(feature)
      feature = null
    }

    override def hasNext: Boolean = reader.hasNext

    override def next(): SimpleFeature = {
      feature = reader.next() // note: our reader returns a mutable copy of the feature
      partition = schemes.map(_.getPartition(feature))
      feature
    }

    override def flush(): Unit = FlushQuietly.raise(appenders.values.toSeq ++ modifiers.values ++ deleters.values)

    override def close(): Unit = CloseQuietly.raise(Seq(reader) ++ appenders.values ++ modifiers.values ++ deleters.values)
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

  /**
   * Gathers partition metadata for a file
   */
  class StorageFileObserver(sft: SimpleFeatureType) extends FileSystemObserver with LazyLogging {

    import scala.collection.JavaConverters._

    private var count: Long = 0L

    private val spatialBounds = sft.spatialBounds().map(_ -> new Envelope())

    private val nonSpatialBounds = sft.nonSpatialBounds().flatMap { i =>
      val binding = sft.getDescriptor(i).getType.getBinding
      AttributeIndexKey.TypeRegistry.getAllEncoders.asScala.find(_.resolves().isAssignableFrom(binding)) match {
        case Some(encoder) => Some(AttributeBoundsBuilder(i, encoder.asInstanceOf[TypeEncoder[AnyRef, String]]))
        case None =>
          logger.warn(
            s"Can't find an encoder for attribute ${sft.getDescriptor(i).getLocalName} of type ${binding.getSimpleName} - " +
              "will not track bounds")
          None
      }
    }

    private var sorted: Seq[(Int, Ordering[AnyRef], AnyRef)] = Range(0, sft.getAttributeCount).flatMap { i =>
      val binding = sft.getDescriptor(i).getType.getBinding
      if (classOf[Comparable[_]].isAssignableFrom(binding)) {
        Some((i, Ordering.ordered[AnyRef](_.asInstanceOf[Comparable[AnyRef]]), null))
      } else {
        None
      }
    }

    /**
     * Get the file metadata that has been gathered so far
     *
     * @param path file path
     * @param partition file partition
     * @param action file action
     * @return
     */
    def file(path: String, partition: Partition, action: StorageFileAction): StorageFile = {
      val spatial = spatialBounds.flatMap { case (i, env) => SpatialBounds(i, env) }
      val nonSpatial = nonSpatialBounds.flatMap(_.build())
      val sort = sorted.map(_._1)
      StorageFile(path, partition, count, action, spatial, nonSpatial, sort)
    }

    override def apply(feature: SimpleFeature): Unit = {
      count += 1L
      spatialBounds.foreach { case (i, env) =>
        val geom = feature.getAttribute(i).asInstanceOf[Geometry]
        if (geom != null) {
          env.expandToInclude(geom.getEnvelopeInternal)
        }
      }
      nonSpatialBounds.foreach(_.apply(feature))
      if (sorted.nonEmpty) {
        sorted = sorted.flatMap { case (i, ordering, last) =>
          val next = feature.getAttribute(i)
          if (last == null || (next != null && ordering.compare(next, last) < 0)) {
            Some((i, ordering, next))
          } else {
            None
          }
        }
      }
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  /**
   * Observer to add a file to the metadata upon closing
   *
   * @param metadata metadata
   * @param path file path
   * @param partition file partition
   * @param action file action
   */
  private class MetadataObserver(metadata: StorageMetadata, path: String, partition: Partition, action: StorageFileAction)
    extends FileSystemObserver {

    private val delegate = new StorageFileObserver(metadata.sft)

    override def apply(feature: SimpleFeature): Unit = delegate.apply(feature)
    override def flush(): Unit = {}
    override def close(): Unit = metadata.addFile(delegate.file(path, partition, action))
  }

  /**
   * Builds up attribute-level bounds
   *
   * @param i attribute index
   * @param lexicoder lexicoder for the attribute type
   */
  private case class AttributeBoundsBuilder(i: Int, lexicoder: TypeEncoder[AnyRef, String]) {

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

    def build(): Option[AttributeBounds] = if (lower == null) { None } else { Some(AttributeBounds(i, lower, upper)) }
  }

  /**
   * Can be used with a MetadataObserver to return storage files instead of writing them directly to the metadata
   *
   * @param sft simple feature type
   * @param schemes partition schemes
   */
  private class FileTracker(val sft: SimpleFeatureType, val schemes: Set[PartitionScheme]) extends StorageMetadata {

    import scala.collection.JavaConverters._

    private val files = new CopyOnWriteArrayList[StorageFile]()

    override def `type`: String = "memory"
    override def addFile(file: StorageFile): Unit = files.add(file)
    override def removeFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
    override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit =
      throw new UnsupportedOperationException()
    override def getFiles(): Seq[StorageFile] = files.asScala.toSeq
    override def getFiles(partition: Partition): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def getFiles(filter: Filter): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }
}
