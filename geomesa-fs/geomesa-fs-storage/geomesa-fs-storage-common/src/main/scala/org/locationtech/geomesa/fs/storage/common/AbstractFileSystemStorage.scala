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
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileSystemPathReader, WriterCallback}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
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

  override def getFilePaths(partition: String): Seq[Path] = {
    val baseDir = StorageUtils.baseDirectory(context.root, partition, metadata.leafStorage)
    val files = metadata.getPartition(partition).map(_.files).getOrElse(Seq.empty)
    files.flatMap { file =>
      val path = new Path(baseDir, file)
      if (PathCache.exists(context.fc, path)) {
        Seq(path)
      } else {
        logger.warn(s"Inconsistent metadata for ${metadata.sft.getTypeName}: $path")
        Seq.empty
      }
    }
  }

  override def getWriter(partition: String): FileSystemWriter = {
    val path = StorageUtils.nextFile(context.root, partition, metadata.leafStorage, extension, FileType.Written)
    PathCache.register(context.fc, path)
    createWriter(path, new AddCallback(partition, path))
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
      val paths = fp.partitions.iterator.flatMap(getFilePaths)
      if (paths.isEmpty) { Iterator.empty } else {
        val filter = Option(fp.filter).filter(_ != Filter.INCLUDE)
        val reader = createReader(filter, transform)
        logger.debug(s"  Reading ${fp.partitions.size} partitions with filter: " +
            filter.map(ECQL.toCQL).getOrElse("INCLUDE"))
        logger.trace(s"  Filter: ${filter.map(ECQL.toCQL).getOrElse("INCLUDE")} Partitions: " +
            fp.partitions.mkString(", "))
        Iterator.single(reader -> paths)
      }
    }

    if (readers.isEmpty) {
      CloseableIterator.empty
    } else {
      FileSystemThreadedReader(readers, threads)
    }
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
        def threaded = FileSystemThreadedReader(Iterator.single(reader -> toCompact.toIterator), threads)
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
        toCompact.foreach { f =>
          if (!context.fc.delete(f, false)) {
            failures.append(f)
          }
          PathCache.invalidate(context.fc, f)
        }

        if (failures.nonEmpty) {
          logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
        }

        logger.debug(s"Compacted $written records into file $path")
      }
    }
  }

  class AddCallback(partition: String, file: Path) extends WriterCallback {
    override def onClose(bounds: Envelope, count: Long): Unit =
      metadata.addPartition(PartitionMetadata(partition, Seq(file.getName), PartitionBounds(bounds), count))
  }

  class CompactCallback(partition: String, file: Path, replaced: Seq[Path]) extends WriterCallback {
    override def onClose(bounds: Envelope, count: Long): Unit = {
      val partitionBounds = PartitionBounds(bounds)
      metadata.removePartition(PartitionMetadata(partition, replaced.map(_.getName), partitionBounds, count))
      metadata.addPartition(PartitionMetadata(partition, Seq(file.getName), partitionBounds, count))
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
