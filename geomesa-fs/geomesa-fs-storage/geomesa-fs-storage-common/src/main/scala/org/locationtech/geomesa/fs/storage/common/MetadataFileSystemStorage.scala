/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.MetadataFileSystemStorage.WriterCallback
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ListBuffer

/**
  * Base class for handling file system metadata
  *
  * @param metadata metadata
  */
abstract class MetadataFileSystemStorage(conf: Configuration,
                                         metadata: org.locationtech.geomesa.fs.storage.api.StorageMetadata)
    extends FileSystemStorage with MethodProfiling with LazyLogging {

  import scala.collection.JavaConverters._

  protected def extension: String

  protected def createWriter(sft: SimpleFeatureType, file: Path, callback: WriterCallback): FileSystemWriter

  protected def createReader(sft: SimpleFeatureType,
                             filter: Option[Filter],
                             transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader

  override def getMetadata: org.locationtech.geomesa.fs.storage.api.StorageMetadata = metadata

  override def getPartitions: java.util.List[PartitionMetadata] = metadata.getPartitions

  override def getPartitions(filter: Filter): java.util.List[PartitionMetadata] = {
    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val all = getPartitions
    if (filter == Filter.INCLUDE) { all } else {
      val coveringPartitions = new java.util.HashSet(metadata.getPartitionScheme.getPartitions(filter))
      if (!coveringPartitions.isEmpty) {
        val iter = all.iterator()
        while (iter.hasNext) {
          if (!coveringPartitions.contains(iter.next.name)) {
            iter.remove()
          }
        }
      }
      all
    }
  }

  override def getPartition(feature: SimpleFeature): String = metadata.getPartitionScheme.getPartition(feature)

  override def getWriter(partition: String): FileSystemWriter = {
    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(metadata.getRoot, partition, leaf, extension, FileType.Written)
    PathCache.register(metadata.getFileContext, dataPath)
    createWriter(metadata.getSchema, dataPath, new AddCallback(partition, dataPath))
  }

  override def getReader(partitions: java.util.List[String], query: Query): FileSystemReader =
    getReader(partitions, query, 1)

  override def getReader(partitions: java.util.List[String], query: Query, threads: Int): FileSystemReader = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    // TODO ask the partition manager the geometry is fully covered?

    val sft = metadata.getSchema
    val q = QueryRunner.default.configureQuery(sft, query)
    val filter = Option(q.getFilter).filter(_ != Filter.INCLUDE)
    val transform = q.getHints.getTransform

    val paths = partitions.iterator.asScala.flatMap(getFilePaths(_).asScala)

    val reader = createReader(sft, filter, transform)

    logger.debug(s"Threading the read of ${partitions.size} partitions with $threads reader threads")

    FileSystemThreadedReader(reader, paths, threads)
  }

  override def getFilePaths(partition: String): java.util.List[Path] = {
    Option(metadata.getPartition(partition)).map(_.files()).filterNot(_.isEmpty) match {
      case None => Collections.emptyList()
      case Some(files) =>
        val partitionPath = StorageUtils.partitionPath(metadata.getRoot, partition)
        val baseDir = if (metadata.getPartitionScheme.isLeafStorage) { partitionPath.getParent } else { partitionPath }
        val paths = new java.util.ArrayList[Path](files.size())

        val iter = files.iterator
        while (iter.hasNext) {
          val path = new Path(baseDir, iter.next)
          if (PathCache.exists(metadata.getFileContext, path)) {
            paths.add(path)
          } else {
            logger.warn(s"Inconsistent metadata for ${metadata.getSchema.getTypeName}: $path")
          }
        }
        paths
    }
  }

  override def compact(partition: String): Unit = compact(partition, 1)

  override def compact(partition: String, threads: Int): Unit = {
    val toCompact = getFilePaths(partition).asScala

    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(metadata.getRoot, partition, leaf, extension, FileType.Compacted)

    val sft = metadata.getSchema

    logger.debug(s"Compacting data files: [${toCompact.mkString(", ")}] to into file $dataPath")

    var written = 0L

    val reader = createReader(sft, None, None)
    def threaded = FileSystemThreadedReader(reader, toCompact.toIterator, threads)
    val callback = new CompactCallback(partition, dataPath, toCompact)

    WithClose(createWriter(sft, dataPath, callback), threaded) { case (writer, features) =>
      while (features.hasNext) {
        writer.write(features.next())
        written += 1
      }
    }
    PathCache.register(metadata.getFileContext, dataPath)

    logger.debug(s"Wrote compacted file $dataPath")

    logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")

    val failures = ListBuffer.empty[Path]
    toCompact.foreach { f =>
      if (!metadata.getFileContext.delete(f, false)) {
        failures.append(f)
      }
      PathCache.invalidate(metadata.getFileContext, f)
    }

    if (failures.nonEmpty) {
      logger.error(s"Failed to delete some files: [${failures.mkString(", ")}]")
    }

    logger.debug("Compacting metadata")

    metadata.compact()

    logger.debug(s"Compacted $written records into file $dataPath")
  }

  class AddCallback(partition: String, file: Path) extends WriterCallback {
    override def onClose(count: Long, bounds: Envelope): Unit =
      metadata.addPartition(new PartitionMetadata(partition, Collections.singletonList(file.getName), count, bounds))
  }

  class CompactCallback(partition: String, file: Path, replaced: Seq[Path]) extends AddCallback(partition, file) {
    override def onClose(count: Long, bounds: Envelope): Unit = {
      metadata.removePartition(new PartitionMetadata(partition, replaced.map(_.getName).asJava, count, bounds))
      super.onClose(count, bounds)
    }
  }
}

object MetadataFileSystemStorage {
  trait WriterCallback {
    def onClose(count: Long, bounds: Envelope): Unit
  }
}
