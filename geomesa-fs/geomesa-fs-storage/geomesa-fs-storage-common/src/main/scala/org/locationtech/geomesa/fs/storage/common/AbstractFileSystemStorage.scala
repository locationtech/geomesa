/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.GeometryDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemPathReader, FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.api.StorageMetadata._
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.api.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.WriterConfig
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, FileSizeEstimator, FlushQuietly}
import org.locationtech.jts.geom.{Envelope, Geometry}

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
    * @param observer observer to report stats on the data written
    * @return
    */
  protected def createWriter(file: Path, observer: FileSystemObserver): FileSystemWriter

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

//  override def getFilePaths(partition: String): Seq[StorageFilePath] = {
//    metadata.getPartition(partition) match {
//      case None => Seq.empty
//      case Some(p) =>
//        val baseDir = StorageUtils.baseDirectory(context.root, partition, metadata.leafStorage)
//        p.files.flatMap { file =>
//          val path = new Path(baseDir, file.name)
//          if (PathCache.exists(context.fs, path)) {
//            Seq(StorageFilePath(file, path))
//          } else {
//            logger.warn(s"Inconsistent metadata for ${metadata.sft.getTypeName}: $path")
//            Seq.empty
//          }
//        }
//    }
//  }

  override def getReader(original: Query, threads: Int): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val query = QueryRunner.configureQuery(metadata.sft, original)
    val transform = query.getHints.getTransform
    val filter = Option(query.getFilter).getOrElse(Filter.INCLUDE)

    // each partition must be read separately, to ensure modifications are handled correctly
    val files = metadata.getFiles(filter).groupBy(_.file.partition).values

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(original.getFilter)}")
    logger.debug(s"  Transforms: " + query.getHints.getTransformDefinition.map { t =>
      if (t.isEmpty) { "empty" } else { t } }.getOrElse("none"))
    logger.debug(s"  Threading the read of ${files.size} files with $threads reader threads")

    if (files.isEmpty) {
      CloseableIterator.empty
    } else {
      val readers = files.iterator.map { sff =>
        logger.debug(s"  Reading ${sff.size} files with filter: ${sff.head.filter.fold("INCLUDE")(ECQL.toCQL)}")
        createReader(sff.head.filter, transform) -> sff.map(_.file)
      }
      FileSystemThreadedReader(readers, threads)
    }
  }

  override def getWriter(partition: Partition): FileSystemWriter =
    createWriter(partition, StorageFileAction.Append, FileType.Written)

  override def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    new FileSystemUpdateWriterImpl(getReader(new Query(metadata.sft.getTypeName, filter), threads))

  override def compact(partition: Partition, fileSize: Option[Long], threads: Int): Unit = {
    // TODO
//    val target = targetSize(fileSize)
//    partition.map(Seq(_)).getOrElse(metadata.getPartitions().map(_.name)).foreach { partition =>
//      val paths = getFilePaths(partition)
//      val toCompact = target match {
//        case None => paths
//        case Some(t) =>
//          paths.filter { p =>
//            if (fileIsSized(p.path, t)) {
//              logger.debug(s"Skipping compaction for file [${p.path}] (already target size)")
//              false
//            } else {
//              true
//            }
//          }
//      }
//
//      if (toCompact.isEmpty) {
//        logger.debug("Skipping compaction - no files to compact")
//      } else if (toCompact.lengthCompare(1) == 0 && target.forall(fileIsSized(toCompact.head.path, _))) {
//        logger.debug(s"Skipping compaction for single data file [${toCompact.mkString}]")
//      } else {
//        logger.debug(s"Compacting data files: [${toCompact.mkString(", ")}]")
//
//        var written = 0L
//        val bounds = new Envelope()
//
//        val reader = createReader(None, None)
//
//        def writer: FileSystemWriter =
//          createWriter(partition, StorageFileAction.Append, FileType.Compacted, target)
//        def threaded: CloseableIterator[SimpleFeature] =
//          FileSystemThreadedReader(Iterator.single(reader -> toCompact), threads)
//
//        WithClose(writer, threaded) { case (writer, features) =>
//          while (features.hasNext) {
//            val feature = features.next()
//            writer.write(feature)
//            written += 1
//            val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
//            if (geom != null) {
//              bounds.expandToInclude(geom.getEnvelopeInternal)
//            }
//          }
//        }
//
//        logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")
//        metadata.removePartition(
//          PartitionMetadata(partition, toCompact.map(_.file), PartitionBounds(bounds), written))
//
//        val failures = ListBuffer.empty[Path]
//        toCompact.foreach { file =>
//          if (!context.fs.delete(file.path, false)) {
//            failures.append(file.path)
//          }
//          PathCache.invalidate(context.fs, file.path)
//        }
//
//        if (failures.nonEmpty) {
//          logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
//        }
//
//        logger.debug(s"Compacted $written records")
//      }
//    }
  }

  /**
   * Create a new writer
   *
   * @param partition partition being written to
   * @param action write type
   * @param fileType file type
   * @param targetFileSize target file size
   * @return
   */
  private def createWriter(
      partition: Partition,
      action: StorageFileAction,
      fileType: FileType,
      targetFileSize: Option[Long] = None): FileSystemWriter = {

    def pathAndObserver: WriterConfig = {
      val file = StorageUtils.nextFile(metadata.sft.getTypeName, fileType, extension)
      val path = new Path(context.root, file)
      val updateObserver = new UpdateObserver(partition, file, action)
      val observer = if (observers.isEmpty) { updateObserver } else {
        new CompositeObserver(observers.map(_.apply(path)).+:(updateObserver))
      }
      WriterConfig(path, observer)
    }

    targetSize(targetFileSize) match {
      case None => createWriter(pathAndObserver)
      case Some(s) => new ChunkedFileSystemWriter(Iterator.continually(pathAndObserver), estimator(s))
    }
  }

  private def createWriter(config: WriterConfig): FileSystemWriter = createWriter(config.path, config.observer)

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
        writer = createWriter(config)
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

    private val modifiers = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]
    private val deleters = scala.collection.mutable.Map.empty[Set[PartitionKey], FileSystemWriter]

    private var feature: SimpleFeature = _
    private var partition: Set[PartitionKey] = _

    override def write(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
      }
      val update = metadata.schemes.map(s => PartitionKey(s.name, s.getPartition(feature)))
      if (update != partition) {
        // add a delete marker in the old partition, since we only track updates per-partition
        deleters.getOrElseUpdate(partition, createWriter(Partition(partition), StorageFileAction.Delete, FileType.Deleted)).write(feature)
      }
      modifiers.getOrElseUpdate(update, createWriter(Partition(update), StorageFileAction.Modify, FileType.Modified)).write(feature)
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
      partition = metadata.schemes.map(s => PartitionKey(s.name, s.getPartition(feature)))
      feature
    }

    override def flush(): Unit = FlushQuietly.raise(modifiers.values.toSeq ++ deleters.values)

    override def close(): Unit = CloseQuietly.raise(Seq(reader) ++ modifiers.values ++ deleters.values ++ observers)
  }

  /**
    * Writes partition data to the metadata
    *
    * @param partition partition being written
    * @param file file being written
    * @param action file type
    */
  private class UpdateObserver(partition: Partition, file: String, action: StorageFileAction) extends FileSystemObserver {

    import scala.collection.JavaConverters._

    private var count: Long = 0L
    private val spatialBounds =
      metadata.sft.getAttributeDescriptors.asScala.toSeq.collect {
        case g: GeometryDescriptor => metadata.sft.indexOf(g.getLocalName) -> new Envelope()
      }
    // TODO track attribute bounds for each file (based on indexed attributes?)
    private val nonSpatialBounds = Seq.empty[AttributeBounds]

    override def apply(feature: SimpleFeature): Unit = {
      count += 1L
      spatialBounds.foreach { case (i, env) =>
        val geom = feature.getAttribute(i).asInstanceOf[Geometry]
        if (geom != null) {
          env.expandToInclude(geom.getEnvelopeInternal)
        }
      }
    }

    override def flush(): Unit = {}

    override def close(): Unit = {
      val spatial = spatialBounds.flatMap { case (i, env) => SpatialBounds(i, env) }
      val sf = StorageFile(file, partition, count, action, spatial, nonSpatialBounds)
      metadata.addFile(sf)
    }
  }
}

object AbstractFileSystemStorage {

  private case class WriterConfig(path: Path, observer: FileSystemObserver)
}
