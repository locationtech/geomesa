/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.data.Query
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.geotools.FeatureUtils
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
                                         metadata: org.locationtech.geomesa.fs.storage.api.FileMetadata)
    extends FileSystemStorage with MethodProfiling with LazyLogging {

  protected def extension: String

  protected def createWriter(sft: SimpleFeatureType, file: Path): FileSystemWriter

  protected def createReader(sft: SimpleFeatureType,
                             filter: Option[Filter],
                             transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader

  override def getMetadata: org.locationtech.geomesa.fs.storage.api.FileMetadata = metadata

  override def getPartitions: java.util.List[String] = metadata.getPartitions

  override def getPartitions(filter: Filter): java.util.List[String] = {
    import scala.collection.JavaConversions._

    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val all = getPartitions
    if (filter == Filter.INCLUDE) { all } else {
      val coveringPartitions = metadata.getPartitionScheme.getPartitions(filter)
      if (coveringPartitions.isEmpty) {
        all // TODO should this ever happen?
      } else {
        all.intersect(coveringPartitions)
      }
    }
  }

  override def getPartition(feature: SimpleFeature): String = metadata.getPartitionScheme.getPartition(feature)

  override def getWriter(partition: String): FileSystemWriter = {
    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(metadata.getRoot, partition, leaf, extension, FileType.Written)
    metadata.addFile(partition, dataPath.getName)
    PathCache.register(metadata.getFileContext, dataPath)
    createWriter(metadata.getSchema, dataPath)
  }

  override def getReader(partitions: java.util.List[String], query: Query): FileSystemReader =
    getReader(partitions, query, 1)

  override def getReader(partitions: java.util.List[String], query: Query, threads: Int): FileSystemReader = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConverters._

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
    import scala.collection.JavaConverters._

    val partitionPath = StorageUtils.partitionPath(metadata.getRoot, partition)
    val baseDir = if (metadata.getPartitionScheme.isLeafStorage) { partitionPath.getParent } else { partitionPath }

    metadata.getFiles(partition).asScala.map(new Path(baseDir, _)).filter { p =>
      if (PathCache.exists(metadata.getFileContext, p)) { true } else {
        logger.warn(s"Inconsistent metadata for ${metadata.getSchema.getTypeName}: $p")
        false
      }
    }.asJava
  }

  override def compact(partition: String): Unit = compact(partition, 1)

  override def compact(partition: String, threads: Int): Unit = {
    import scala.collection.JavaConverters._

    val toCompact = getFilePaths(partition).asScala

    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(metadata.getRoot, partition, leaf, extension, FileType.Compacted)

    val sft = metadata.getSchema

    logger.debug(s"Compacting data files: [${toCompact.mkString(", ")}] to into file $dataPath")

    var written = 0L

    val reader = createReader(sft, None, None)
    def threaded = FileSystemThreadedReader(reader, toCompact.toIterator, threads)

    WithClose(createWriter(sft, dataPath), threaded) { case (writer, features) =>
      while (features.hasNext) {
        writer.write(features.next())
        writer.flush()
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

    logger.debug(s"Updating metadata for type ${metadata.getSchema.getTypeName}")
    metadata.replaceFiles(partition, toCompact.map(_.getName).asJava, dataPath.getName)

    logger.debug(s"Compacted $written records into file $dataPath")
  }

  override def updateMetadata(): Unit = {
    profile {
      val fc = metadata.getFileContext
      val scheme = metadata.getPartitionScheme
      metadata.setFiles(StorageUtils.partitionsAndFiles(fc, metadata.getRoot, scheme, extension, cache = false))
    } ((_, time) => logger.debug(s"Metadata update took ${time}ms."))
  }
}

