/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.api.StorageMetadata._
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileSystemPathReader, WriterCallback}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ListBuffer

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
  ) extends FileSystemStorage with MethodProfiling with LazyLogging {

  /**
    * Create a writer for the given file
    *
    * @param file file to write to
    * @param callback callback to report stats on the data written
    * @return
    */
  protected def createWriter(file: Path, callback: WriterCallback): FileSystemWriter

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

  override def getFilePaths(partition: String): Seq[StorageFilePath] = {
    val baseDir = StorageUtils.baseDirectory(context.root, partition, metadata.leafStorage)
    val files = metadata.getPartition(partition).map(_.files).getOrElse(Seq.empty)
    files.flatMap { file =>
      val path = new Path(baseDir, file.name)
      if (PathCache.exists(context.fc, path)) {
        Seq(StorageFilePath(file, path))
      } else {
        logger.warn(s"Inconsistent metadata for ${metadata.sft.getTypeName}: $path")
        Seq.empty
      }
    }
  }

  override def getReader(original: Query, partition: Option[String], threads: Int): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val query = QueryRunner.configureDefaultQuery(metadata.sft, original)
    val transform = query.getHints.getTransform
    val filter = Option(query.getFilter).getOrElse(Filter.INCLUDE)

    val filters = getPartitionFilters(filter, partition)

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(original.getFilter)}")
    logger.debug(s"  Transforms: " + query.getHints.getTransformDefinition.map { t =>
      if (t.isEmpty) { "empty" } else { t } }.getOrElse("none"))
    logger.debug(s"  Threading the read of ${filters.map(_.partitions.size).sum} partitions with " +
        s"$threads reader threads")

    val readers = filters.iterator.flatMap { fp =>
      lazy val reader = {
        val filter = Option(fp.filter).filter(_ != Filter.INCLUDE)
        val reader = createReader(filter, transform)
        logger.debug(s"  Reading ${fp.partitions.size} partitions with filter: " +
            filter.map(ECQL.toCQL).getOrElse("INCLUDE"))
        logger.trace(s"  Filter: ${filter.map(ECQL.toCQL).getOrElse("INCLUDE")} Partitions: " +
            fp.partitions.mkString(", "))
        reader
      }
      // each partition must be read separately, to ensure modifications are handled correctly
      fp.partitions.iterator.flatMap { p =>
        val files = getFilePaths(p)
        if (files.isEmpty) { Iterator.empty } else { Iterator.single(reader -> files) }
      }
    }

    if (readers.isEmpty) {
      CloseableIterator.empty
    } else {
      FileSystemThreadedReader(readers, threads)
    }
  }

  override def getWriter(partition: String): FileSystemWriter = createWriter(partition, StorageFileAction.Append)

  override def getWriter(filter: Filter, partition: Option[String], threads: Int): FileSystemUpdateWriter = {
    val query = new Query(metadata.sft.getTypeName, filter)
    new FileSystemUpdateWriterImpl(getReader(query, partition, threads), partition)
  }

  override def compact(partition: Option[String], threads: Int): Unit = {
    partition.map(Seq(_)).getOrElse(metadata.getPartitions().map(_.name)).foreach { partition =>
      val toCompact = getFilePaths(partition)

      if (toCompact.lengthCompare(2) < 0) {
        logger.debug(s"Skipping compaction for single data file: ${toCompact.mkString(", ")}")
      } else {
        val path = StorageUtils.nextFile(context.root, partition, metadata.leafStorage, extension, FileType.Compacted)

        logger.debug(s"Compacting data files: [${toCompact.mkString(", ")}] to into file $path")

        var written = 0L

        val reader = createReader(None, None)
        def threaded = FileSystemThreadedReader(Iterator.single(reader -> toCompact), threads)
        val callback = new CompactCallback(partition, path, toCompact)

        WithClose(createWriter(path, callback), threaded) { case (writer, features) =>
          while (features.hasNext) {
            writer.write(features.next())
            written += 1
          }
        }
        PathCache.register(context.fc, path)

        logger.debug(s"Wrote compacted file $path")
        logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")

        val failures = ListBuffer.empty[Path]
        toCompact.foreach { file =>
          if (!context.fc.delete(file.path, false)) {
            failures.append(file.path)
          }
          PathCache.invalidate(context.fc, file.path)
        }

        if (failures.nonEmpty) {
          logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
        }

        logger.debug(s"Compacted $written records into file $path")
      }
    }
  }

  /**
    * Create a new writer
    *
    * @param partition partition being written to
    * @param action write type
    * @return
    */
  private def createWriter(partition: String, action: StorageFileAction): FileSystemWriter = {
    val fileType = action match {
      case StorageFileAction.Append => FileType.Written
      case StorageFileAction.Modify => FileType.Modified
      case StorageFileAction.Delete => FileType.Deleted
      case _ => throw new NotImplementedError(s"Unexpected storage action type: $action")
    }
    val path = StorageUtils.nextFile(context.root, partition, metadata.leafStorage, extension, fileType)
    PathCache.register(context.fc, path)
    createWriter(path, new MetadataCallback(partition, path, action))
  }

  /**
    * Update writer implementation
    *
    * @param reader reader for features to update
    * @param readPartition read partition, if known
    */
  class FileSystemUpdateWriterImpl(reader: CloseableFeatureIterator, readPartition: Option[String])
      extends FileSystemUpdateWriter {

    private val modifiers = scala.collection.mutable.Map.empty[String, FileSystemWriter]
    private val deleters = scala.collection.mutable.Map.empty[String, FileSystemWriter]

    private var feature: SimpleFeature = _
    private var partition: String = _

    override def write(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'write'")
      }
      val update = metadata.scheme.getPartitionName(feature)
      if (update != partition) {
        // add a delete marker in the old partition, since we only track updates per-partition
        deleters.getOrElseUpdate(partition, createWriter(partition, StorageFileAction.Delete)).write(feature)
      }
      modifiers.getOrElseUpdate(update, createWriter(update, StorageFileAction.Modify)).write(feature)
      feature = null
    }

    override def remove(): Unit = {
      if (feature == null) {
        throw new IllegalArgumentException("Must call 'next' before calling 'remove'")
      }
      deleters.getOrElseUpdate(partition, createWriter(partition, StorageFileAction.Delete)).write(feature)
      feature = null
    }

    override def hasNext: Boolean = reader.hasNext

    override def next(): SimpleFeature = {
      feature = reader.next() // note: our reader returns a mutable copy of the feature
      partition = readPartition.getOrElse(metadata.scheme.getPartitionName(feature))
      feature
    }

    override def flush(): Unit = FlushQuietly(modifiers.values.toSeq ++ deleters.values).foreach(e => throw e)

    override def close(): Unit =
      CloseQuietly(Seq(reader) ++ modifiers.values ++ deleters.values).foreach(e => throw e)
  }

  /**
    * Writes partition data to the metadata
    *
    * @param partition partition being written
    * @param file file being written
    * @param action file type
    */
  class MetadataCallback(partition: String, file: Path, action: StorageFileAction) extends WriterCallback {
    override def onClose(bounds: Envelope, count: Long): Unit = {
      val files = Seq(StorageFile(file.getName, System.currentTimeMillis(), action))
      metadata.addPartition(PartitionMetadata(partition, files, PartitionBounds(bounds), count))
    }
  }

  /**
    * Writes compacted partition data to the metadata
    *
    * @param partition partition being compacted
    * @param file compacted file being written
    * @param replaced files being replaced
    */
  class CompactCallback(partition: String, file: Path, replaced: Seq[StorageFilePath]) extends WriterCallback {
    override def onClose(bounds: Envelope, count: Long): Unit = {
      val partitionBounds = PartitionBounds(bounds)
      metadata.removePartition(PartitionMetadata(partition, replaced.map(_.file), partitionBounds, count))
      val added = Seq(StorageFile(file.getName, System.currentTimeMillis(), StorageFileAction.Append))
      metadata.addPartition(PartitionMetadata(partition, added, partitionBounds, count))
    }
  }
}

object AbstractFileSystemStorage {

  trait WriterCallback {
    def onClose(bounds: Envelope, count: Long): Unit
  }

  trait FileSystemPathReader {
    def read(path: Path): CloseableIterator[SimpleFeature]
  }

  trait MetadataObservingFileSystemWriter extends FileSystemWriter {

    def callback: WriterCallback

    private var count: Long = 0L
    private val bounds: Envelope = new Envelope()

    abstract override def write(feature: SimpleFeature): Unit = {
      super.write(feature)
      // Update internal count/bounds/etc
      count += 1L
      bounds.expandToInclude(feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)
    }

    abstract override def close(): Unit = {
      super.close()
      callback.onClose(bounds, count)
    }
  }
}
