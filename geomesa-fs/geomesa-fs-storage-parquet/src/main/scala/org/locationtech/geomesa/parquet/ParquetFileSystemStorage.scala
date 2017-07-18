/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.net.URI
import java.util.Collections
import java.util.concurrent.Callable
import java.{io, util}

import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, PartitionScheme, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage._
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, io.Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals(ParquetEncoding)
  }

  override def build(params: util.Map[String, io.Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    val conf = new Configuration

    if (params.containsKey(ParquetCompressionOpt)) {
      conf.set(ParquetCompressionOpt, params.get(ParquetCompressionOpt).asInstanceOf[String])
    } else if (System.getProperty(ParquetCompressionOpt) != null) {
      conf.set(ParquetCompressionOpt, System.getProperty(ParquetCompressionOpt))
    }

    new ParquetFileSystemStorage(root, root.getFileSystem(conf), conf, params)
  }
}

/**
  *
  * @param root the root of this file system for a specifid SimpleFeatureType
  * @param fs
  */
class ParquetFileSystemStorage(root: Path,
                               fs: FileSystem,
                               conf: Configuration,
                               dsParams: util.Map[String, io.Serializable]) extends FileSystemStorage with LazyLogging {

  private val typeNames: mutable.ListBuffer[String] = {
    val s = System.currentTimeMillis
    val b = mutable.ListBuffer.empty[String]
    if (fs.exists(root)) {
      fs.listStatus(root).filter(_.isDirectory).map(_.getPath.getName).foreach(b += _)
    }
    val e = System.currentTimeMillis
    logger.info(s"Type loading took ${e-s}ms")
    b
  }

  override def listTypeNames(): util.List[String] = Collections.unmodifiableList(typeNames)

  private def metadata(typeName: String) =
    ParquetFileSystemStorage.MetadataCache.get((root, typeName), new Callable[Metadata] {
      override def call(): Metadata = {
        val start = System.currentTimeMillis()

        val typePath = new Path(root, typeName)
        val metaFile = new Path(typePath, MetadataFileName)
        val metadata = FileMetadata.read(fs, metaFile, conf)

        val end = System.currentTimeMillis()
        logger.debug(s"Loaded metadata in ${end-start}ms for type $typeName")
        metadata
      }
    })

  override def getFeatureType(typeName: String): SimpleFeatureType =
    metadata(typeName).getSimpleFeatureType

  override def createNewFeatureType(sft: SimpleFeatureType, scheme: PartitionScheme): Unit = {
    val typeName = sft.getTypeName

    if (!typeNames.contains(typeName)) {
      MetadataCache.put((root, typeName), {
          val typePath = new Path(root, typeName)
          val scheme = PartitionScheme.extractFromSft(sft)
          val metaPath = new Path(typePath, MetadataFileName)
          val metadata = FileMetadata.create(fs, metaPath, sft, ParquetEncoding, scheme, conf)
          typeNames += typeName
          metadata
      })
    } else {
      val newDesc = sft.getAttributeDescriptors
      val existing = metadata(typeName).getSimpleFeatureType.getAttributeDescriptors
      for (i <- 0 until newDesc.length) {
        require(newDesc(i) == existing(i), s"New Attribute Descriptor ${newDesc(i).getLocalName} is not" +
          s"equivalent to existing descriptor ${existing(i).getLocalName} at index $i")
      }
    }
  }

  override def listFeatureTypes: util.List[SimpleFeatureType] = typeNames.map(getFeatureType)

  override def listPartitions(typeName: String): util.List[String] =
    metadata(typeName).getPartitions

  // TODO ask the parition manager the geometry is fully covered?
  override def getPartitionReader(typeName: String, q: Query, partition: String): FileSystemPartitionIterator = {
    val sft = metadata(typeName).getSimpleFeatureType

    import org.locationtech.geomesa.index.conf.QueryHints._
    QueryPlanner.setQueryTransforms(q, sft)

    val transformSft = q.getHints.getTransformSchema.getOrElse(sft)

    val fc = new FilterConverter(transformSft).convert(q.getFilter)
    val parquetFilter =
      fc._1
        .map(FilterCompat.get)
        .getOrElse(FilterCompat.NOOP)

    logger.debug(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")

    import scala.collection.JavaConversions._
    val iters = getPaths(sft.getTypeName, partition).toIterator.map(u => new Path(u)).map { path =>
      if (!fs.exists(path)) {
        new EmptyFsIterator(partition)
      }
      else {
        val support = new SimpleFeatureReadSupport
        SimpleFeatureReadSupport.setSft(transformSft, conf)

        conf.set("parquet.filter.dictionary.enabled", "true")
        val builder = ParquetReader.builder[SimpleFeature](support, path)
          .withFilter(parquetFilter)
          .withConf(conf)

        new FilteringIterator(partition, builder, fc._2)
      }
    }
    new MultiIterator(partition, iters)
  }

  override def getWriter(typeName: String, partition: String): FileSystemWriter = {
    new FileSystemWriter {
      private val meta = metadata(typeName)
      private val sft = meta.getSimpleFeatureType

      private val sftConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.setSft(sft, c)
        c
      }
      private val leaf = meta.getPartitionScheme.isLeafStorage
      private val dataPath = StorageUtils.nextFile(fs, root, typeName, partition, leaf, FileExtension)
      private val writer = SimpleFeatureParquetWriter.builder(dataPath, sftConf).build()
      meta.addFile(partition, dataPath.getName)

      override def write(f: SimpleFeature): Unit = writer.write(f)

      override def flush(): Unit = {}

      override def close(): Unit = CloseQuietly(writer)
    }
  }

  override def getPartitionScheme(typeName: String): PartitionScheme =
    metadata(typeName).getPartitionScheme

  private def listStorageFiles(typeName: String): util.Map[String, util.List[String]] = {
    val scheme = getPartitionScheme(typeName)
    val partitions =
      StorageUtils.buildPartitionList(root, fs, typeName, scheme, StorageUtils.SequenceLength, FileExtension)
    import scala.collection.JavaConverters._
    partitions.map { p =>
      val files = StorageUtils.listFiles(fs, root, typeName, p, scheme.isLeafStorage, FileExtension).map(_.getName).asJava
      p -> files
    }.toMap.asJava
  }

  override def getPaths(typeName: String, partition: String): java.util.List[URI] = {
    val scheme = metadata(typeName).getPartitionScheme
    val baseDir = if (scheme.isLeafStorage) {
      StorageUtils.partitionPath(root, typeName, partition).getParent
    } else {
      StorageUtils.partitionPath(root, typeName, partition)
    }
    import scala.collection.JavaConversions._
    val files = metadata(typeName).getFiles(partition)
    files.map(new Path(baseDir, _)).map(_.toUri)
  }

  override def getMetadata(typeName: String): Metadata = metadata(typeName)

  override def updateMetadata(typeName: String): Unit = metadata(typeName).addPartitions(listStorageFiles(typeName))

}

object ParquetFileSystemStorage {
  val ParquetEncoding  = "parquet"
  val FileExtension    = "parquet"
  val MetadataFileName = "metadata.json"

  val ParquetCompressionOpt = "parquet.compression"

  val MetadataCache: Cache[(Path, String), Metadata] = CacheBuilder.newBuilder().build[(Path, String), Metadata]()
}
