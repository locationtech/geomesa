/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import java.io.IOException
import java.net.URI
import java.util.Collections
import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ListBuffer

/**
  * Base class for handling file system metadata
  *
  * @param fs filesystem
  * @param root the root of this file system for a specified SimpleFeatureType
  * @param conf configuration
  */
abstract class MetadataFileSystemStorage(fs: FileSystem, root: Path, conf: Configuration)
    extends FileSystemStorage with MethodProfiling with LazyLogging {

  import MetadataFileSystemStorage.{MetadataCache, MetadataFileName}

  private lazy val typeNames = {
    implicit def complete(result: ListBuffer[String], time: Long): Unit = logger.debug(s"Type loading took ${time}ms")
    profile {
      val names = ListBuffer.empty[String]
      if (fs.exists(root)) {
        fs.listStatus(root).collect { case d if d.isDirectory => d.getPath.getName }.foreach(names += _)
      }
      names
    }
  }

  protected def encoding: String

  protected def extension: String

  protected def createWriter(sft: SimpleFeatureType, file: Path): FileSystemWriter

  protected def createReader(sft: SimpleFeatureType,
                             filter: Option[Filter],
                             transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader

  override def getRoot: URI = root.toUri

  override def getFeatureType(typeName: String): SimpleFeatureType = getMetadata(typeName).getSimpleFeatureType

  override def getTypeNames: java.util.List[String] = {
    import scala.collection.JavaConversions._
    Collections.unmodifiableList(typeNames)
  }

  override def getFeatureTypes: java.util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    typeNames.map(getFeatureType)
  }

  override def createNewFeatureType(sft: SimpleFeatureType, scheme: PartitionScheme): Unit = {
    val typeName = sft.getTypeName

    if (!typeNames.contains(typeName)) {
      val metadata = createFileMetadata(sft, scheme)
      typeNames += typeName
      MetadataCache.put((root, typeName), metadata)
    } else {
      val metadata = getMetadata(typeName)
      val existing = metadata.getSimpleFeatureType
      require(sft.getAttributeCount == existing.getAttributeCount, "New feature type is not equivalent to existing " +
          s"feature type: ${SimpleFeatureTypes.encodeType(sft)} :: ${SimpleFeatureTypes.encodeType(existing)}")
      var i = 0
      while (i < sft.getAttributeCount) {
        require(sft.getDescriptor(i) == existing.getDescriptor(i), "New Attribute Descriptor " +
            s"${sft.getDescriptor(i).getLocalName} is not equivalent to existing descriptor " +
            s"${existing.getDescriptor(i).getLocalName} at index $i")
        i += 1
      }
      require(metadata.getPartitionScheme == scheme,
        s"New partition scheme $scheme is not equivalent to existing scheme ${metadata.getPartitionScheme}")
    }
  }

  override def getPartitionScheme(typeName: String): PartitionScheme = getMetadata(typeName).getPartitionScheme

  override def getPartitions(typeName: String): java.util.List[String] = getMetadata(typeName).getPartitions

  override def getPartitions(typeName: String, query: Query): java.util.List[String] = {
    import scala.collection.JavaConversions._

    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val all = getPartitions(typeName)
    if (query.getFilter == Filter.INCLUDE) { all } else {
      val coveringPartitions = getPartitionScheme(typeName).getCoveringPartitions(query.getFilter)
      if (coveringPartitions.isEmpty) {
        all // TODO should this ever happen?
      } else {
        all.intersect(coveringPartitions)
      }
    }
  }

  override def getPaths(typeName: String, partition: String): java.util.List[URI] = {
    import scala.collection.JavaConversions._

    val metadata = getMetadata(typeName)
    val baseDir = if (metadata.getPartitionScheme.isLeafStorage) {
      StorageUtils.partitionPath(root, typeName, partition).getParent
    } else {
      StorageUtils.partitionPath(root, typeName, partition)
    }
    metadata.getFiles(partition).map(new Path(baseDir, _)).collect { case f if fs.exists(f) => f.toUri }
  }

  override def getMetadata(typeName: String): Metadata = {
    MetadataFileSystemStorage.MetadataCache.get((root, typeName), new Callable[Metadata] {
      override def call(): Metadata = {
        implicit def complete(metadata: Metadata, time: Long): Unit =
          logger.debug(s"Loaded metadata in ${time}ms for type $typeName")

        profile {
          val typePath = new Path(root, typeName)
          val metaFile = new Path(typePath, MetadataFileSystemStorage.MetadataFileName)
          FileMetadata.read(fs, metaFile, conf)
        }
      }
    })
  }

  override def updateMetadata(typeName: String): Unit = {
    implicit def complete(result: Unit, time: Long): Unit =
      logger.debug(s"Metadata Update took ${time}ms.")

    profile {
      val metadata = getMetadata(typeName)
      val scheme = metadata.getPartitionScheme
      val sft = metadata.getSimpleFeatureType
      val parts = StorageUtils.partitionsAndFiles(root, fs, typeName, scheme, extension)

      // Save existing metadata
      backupMetadata(typeName)
      cleanBackups(typeName)

      // Recreate a new metadata file
      val newMetadata = createFileMetadata(sft, metadata.getPartitionScheme)
      MetadataCache.invalidate((root, typeName))
      MetadataCache.put((root, typeName), newMetadata)

      newMetadata.addPartitions(parts)
    }
  }

  override def getWriter(typeName: String, partition: String): FileSystemWriter = {
    val metadata = getMetadata(typeName)
    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(fs, root, typeName, partition, leaf, extension, FileType.Written)

    metadata.addFile(partition, dataPath.getName)

    createWriter(metadata.getSimpleFeatureType, dataPath)
  }

  override def getReader(typeName: String,
                         partitions: java.util.List[String],
                         query: Query): FileSystemReader = getReader(typeName, partitions, query, 1)

  override def getReader(typeName: String,
                         partitions: java.util.List[String],
                         query: Query,
                         threads: Int): FileSystemReader = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConversions._

    // TODO ask the partition manager the geometry is fully covered?

    val metadata = getMetadata(typeName)
    val sft = metadata.getSimpleFeatureType
    val q = QueryRunner.default.configureQuery(sft, query)
    val filter = Option(q.getFilter).filter(_ != Filter.INCLUDE)
    val transform = q.getHints.getTransform

    val paths = partitions.toIterator.flatMap(getPaths(typeName, _).map(new Path(_)))

    val reader = createReader(sft, filter, transform)

    logger.debug(s"Threading the read of ${partitions.size} partitions with $threads reader threads (and 1 writer thread)")

    FileSystemThreadedReader(reader, paths, threads)
  }

  override def compact(typeName: String, partition: String): Unit = compact(typeName, partition, 1)

  override def compact(typeName: String, partition: String, threads: Int): Unit = {
    import scala.collection.JavaConversions._

    val toCompact = getPaths(typeName, partition).map(new Path(_))

    val metadata = getMetadata(typeName)
    val leaf = metadata.getPartitionScheme.isLeafStorage
    val dataPath = StorageUtils.nextFile(fs, root, typeName, partition, leaf, extension, FileType.Compacted)

    val sft = metadata.getSimpleFeatureType

    logger.debug(s"Compacting data files: [${toCompact.mkString(", ")}] to into file $dataPath")

    var written = 0L

    val reader = createReader(sft, None, None)
    def threaded = FileSystemThreadedReader(reader, toCompact.toIterator, threads)

    WithClose(createWriter(sft, dataPath), threaded) { case (writer, features) =>
      while (features.hasNext) {
        writer.write(features.next())
        written += 1
      }
    }

    logger.debug(s"Wrote compacted file $dataPath")

    logger.debug(s"Deleting old files [${toCompact.mkString(", ")}]")

    val failures = ListBuffer.empty[Path]
    toCompact.foreach(f => if (!fs.delete(f, false)) { failures.append(f) })

    if (failures.nonEmpty) {
      logger.warn(s"Failed to delete some files: [${failures.mkString(", ")}]")
    }

    logger.debug(s"Updating metadata for type $typeName")
    updateMetadata(typeName)

    logger.debug(s"Compacted $written records into file $dataPath")
  }

  private def createFileMetadata(sft: SimpleFeatureType, scheme: PartitionScheme) = {
    val typeName = sft.getTypeName
    val typePath = new Path(root, typeName)
    val metaPath = new Path(typePath, MetadataFileName)
    FileMetadata.create(fs, metaPath, sft, encoding, scheme, conf)
  }

  private def backupMetadata(typeName: String): Unit = {
    val typePath = new Path(root, typeName)
    val metaPath = new Path(typePath, MetadataFileName)
    val backupFile = new Path(typePath, s".$MetadataFileName.old.${System.currentTimeMillis()}.${System.nanoTime()}")
    fs.rename(metaPath, backupFile)

    // Because of eventual consistency lets make sure they are there
    var tryNum = 0
    var backupComplete = false

    def waitOnBackup(): Unit = {
      if (fs.exists(backupFile) && !fs.exists(metaPath)) {
        backupComplete = true
      } else {
        tryNum += 1
        Thread.sleep((2 ^ tryNum) * 1000)
      }
    }

    while (!backupComplete && tryNum < 3) {
      waitOnBackup()
    }

    if (!backupComplete) {
      throw new IOException(s"Unable to properly backup metadata after $tryNum tries")
    }
  }

  private def cleanBackups(typeName: String): Unit = {
    val typePath = new Path(root, typeName)
    val fileItr = fs.listFiles(typePath, false)
    val backupFiles = ListBuffer.empty[Path]
    while (fileItr.hasNext) {
      val nextPath = fileItr.next().getPath
      if (nextPath.getName.matches(s"\\.$MetadataFileName\\.old\\.\\d+.*")) {
        backupFiles += nextPath
      }
    }

    // Keep the 5 most recent metadata files and delete the old ones
    backupFiles.sortBy(_.getName).dropRight(5).foreach { p =>
      logger.debug(s"Removing old metadata backup $p")
      fs.delete(p, false)
    }
  }
}

object MetadataFileSystemStorage {

  val MetadataFileName = "metadata.json"

  val MetadataCache: Cache[(Path, String), Metadata] = CacheBuilder.newBuilder().build[(Path, String), Metadata]()
}
