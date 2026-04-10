/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileUtil, Path}
import org.calrissian.mango.types.TypeEncoder
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemPathReader, FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.api.StorageMetadata._
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.api.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileTracker, MetadataObserver, StorageFileObserver, WriterConfig}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, FileSizeEstimator, FlushQuietly, WithClose}
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Base class storage implementations
 *
 * @param context file system context
 * @param metadata metadata
 * @param extension file extension
 */
abstract class AbstractFileSystemStorage(
    val context: FileSystemContext,
    val metadata: StorageMetadata,
    extension: String
  ) extends FileSystemStorage with SizeableFileSystemStorage with LazyLogging {

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
          case o: org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory => o.bridge()
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
   * Create a writer for the given file
   *
   * @param file file to write to
   * @param partition partition being written to
   * @param observer observer to report stats on the data written
   * @return
   */
  protected def createWriter(file: Path, partition: Partition, observer: FileSystemObserver): FileSystemWriter

  /**
    * Create a path reader with the given filter and transform
    *
    * @param filter filter, if any
    * @param transform transform
    * @return
    */
  protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader

  override def getReader(original: Query, threads: Int): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val query = QueryRunner.configureQuery(metadata.sft, original)
    val transform = query.getHints.getTransform
    val filter = Option(query.getFilter).getOrElse(Filter.INCLUDE)

    val files = metadata.getFiles(filter)
    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(original.getFilter)}")
    logger.debug(s"  Transforms: " + query.getHints.getTransformDefinition.map { t =>
      if (t.isEmpty) { "empty" } else { t } }.getOrElse("none"))
    logger.debug(s"  Threading the read of ${files.size} files with $threads reader threads")

    if (files.isEmpty) {
      CloseableIterator.empty
    } else {
      logger.debug(s"  Reading ${files.size} files with filter: ${ECQL.toCQL(filter)}")
      logger.whenTraceEnabled(files.foreach(f => logger.trace(s"    $f")))
      val reader = createReader(Option(filter).filterNot(_ == Filter.INCLUDE), transform)
      FileSystemThreadedReader(reader, files, threads)
    }
  }

  override def getWriter(partition: Partition): FileSystemWriter =
    createWriter(partition, StorageFileAction.Append, FileType.Written)

  override def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    new FileSystemUpdateWriterImpl(getReader(new Query(metadata.sft.getTypeName, filter), threads))

  override def compact(partition: Partition, fileSize: Option[Long], threads: Int): Unit = {
    val target = targetSize(fileSize)
    val files = metadata.getFiles(partition)
    val toCompact = target match {
      case None => files
      case Some(t) =>
        files.filter { f =>
          if (fileIsSized(new Path(context.root, f.file), t)) {
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

      WithClose(createWriter(partition, StorageFileAction.Append, FileType.Compacted, target, fileTracker)) { writer =>
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
        val path = new Path(context.root, file.file)
        if (!context.fs.delete(path, false)) {
          failures.append(file.file)
        }
        PathCache.invalidate(context.fs, path)
      }

      if (failures.nonEmpty) {
        logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
      }

      logger.debug(s"Compacted $written records")
    }
  }

  override def register(file: Path): StorageFile = {
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
      val filePath = StorageUtils.newFilePath(metadata.sft.getTypeName, FileType.Written, encoding)
      observer.file(filePath, partitions.iterator().next(), StorageFileAction.Append)
    }
    if (partitions.size() != 1) {
      throw new IllegalArgumentException(s"File corresponds to multiple partitions: ${partitions.asScala.mkString(" AND ")}")
    }

    val destination = new Path(context.root, storageFile.file)
    logger.debug(s"Copying $file to $destination")
    FileUtil.copy(file.getFileSystem(context.conf), file, context.fs, destination, false, context.conf)

    metadata.addFile(storageFile)
    storageFile
  }

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
      fileType: FileType,
      targetFileSize: Option[Long] = None,
      metadata: StorageMetadata = this.metadata): FileSystemWriter = {

    def pathAndObserver: WriterConfig = {
      val file = StorageUtils.newFilePath(metadata.sft.getTypeName, fileType, extension)
      val path = new Path(context.root, file)
      val updateObserver = new MetadataObserver(metadata, file, partition, action)
      val observer = if (observers.isEmpty) { updateObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(updateObserver))
      }
      WriterConfig(path, partition, observer)
    }

    targetSize(targetFileSize) match {
      case None => val config = pathAndObserver; createWriter(config.path, config.partition, config.observer)
      case Some(s) => new ChunkedFileSystemWriter(Iterator.continually(pathAndObserver), estimator(s))
    }
  }

  /**
   * Writes files up to a given size, then starts a new file
   *
   * @param paths iterator of files to write
   * @param estimator target file size estimator
   */
  private class ChunkedFileSystemWriter(paths: Iterator[WriterConfig], estimator: FileSizeEstimator)
      extends FileSystemWriter {

    private var count = 0L // number of features written
    private var total = 0L // sum size of all finished chunks
    private var remaining = estimator.estimate(0L)

    private var path: Path = _
    private var writer: FileSystemWriter = _

    override def write(feature: SimpleFeature): Unit = {
      if (writer == null) {
        val config = paths.next
        path = config.path
        writer = createWriter(config.path, config.partition, config.observer)
      }
      writer.write(feature)
      count += 1
      remaining -= 1
      if (remaining == 0) {
        writer.close()
        writer = null
        // adjust our estimate to account for the actual bytes written
        total += context.fs.getFileStatus(path).getLen
        estimator.update(total, count)
        remaining = estimator.estimate(0L)
      }
    }

    override def flush(): Unit = if (writer != null) { writer.flush() }

    override def close(): Unit = {
      if (writer != null) {
        writer.close()
      }
      updateFileSize(estimator)
    }
  }

  /**
    * Update writer implementation
    *
    * @param reader reader for features to update
    */
  private class FileSystemUpdateWriterImpl(reader: CloseableFeatureIterator) extends FileSystemUpdateWriter {

    private val appenders = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]
    private val modifiers = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]
    private val deleters = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]

    private var feature: SimpleFeature = _
    private var partition: Set[PartitionKey] = _

    override def write(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
      }
      val update = metadata.schemes.map(_.getPartition(feature))
      if (update == partition) {
        modifiers.getOrElseUpdate(update, createWriter(Partition(update), StorageFileAction.Modify, FileType.Modified)).write(feature)
      } else {
        // add a delete marker in the old partition, and an append in the new one, since we only track updates per-partition
        deleters.getOrElseUpdate(partition, createWriter(Partition(partition), StorageFileAction.Delete, FileType.Deleted)).write(feature)
        appenders.getOrElseUpdate(update, createWriter(Partition(update), StorageFileAction.Append, FileType.Written)).write(feature)
      }
      feature = null
    }

    override def remove(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'remove'")
      }
      deleters.getOrElseUpdate(partition, createWriter(Partition(partition), StorageFileAction.Delete, FileType.Deleted)).write(feature)
      feature = null
    }

    override def hasNext: Boolean = reader.hasNext

    override def next(): SimpleFeature = {
      feature = reader.next() // note: our reader returns a mutable copy of the feature
      partition = metadata.schemes.map(_.getPartition(feature))
      feature
    }

    override def flush(): Unit = FlushQuietly.raise(appenders.values.toSeq ++ modifiers.values ++ deleters.values)

    override def close(): Unit = CloseQuietly.raise(Seq(reader) ++ appenders.values ++ modifiers.values ++ deleters.values ++ observers)
  }
}

object AbstractFileSystemStorage {

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
    override def getFiles(): Seq[StorageFile] = files.asScala
    override def getFiles(partition: Partition): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def getFiles(filter: Filter): Seq[StorageFile] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }

  private case class WriterConfig(path: Path, partition: Partition, observer: FileSystemObserver)
}
