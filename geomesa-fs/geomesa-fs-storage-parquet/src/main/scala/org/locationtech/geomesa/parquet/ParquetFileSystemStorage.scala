/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.net.URI
import java.util.concurrent.Callable
import java.{io, util}

import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, StoragePartition, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, io.Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("parquet")
  }

  override def build(params: util.Map[String, io.Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    val conf = new Configuration
    if (params.containsKey("parquet.compression")) {
      conf.set("parquet.compression", params.get("parquet.compression").asInstanceOf[String])
    } else if (System.getProperty("parquet.compression") != null) {
      conf.set("parquet.compression", System.getProperty("parquet.compression"))
    }
    new ParquetFileSystemStorage(root, root.getFileSystem(conf), conf)
  }
}

/**
  *
  * @param root the root of this file system for a specifid SimpleFeatureType
  * @param fs
  */
class ParquetFileSystemStorage(root: Path,
                               fs: FileSystem,
                               conf: Configuration) extends FileSystemStorage with LazyLogging {

  private val dataFileExtention = "parquet"
  private val schemaFile = "schema.sft"
  private val metaFileName = "metadata"

  private def metadata(typeName: String) =
    ParquetFileSystemStorage.MetadataCache.get((root, typeName), new Callable[Metadata] {
      override def call(): Metadata = {
        val start = System.currentTimeMillis()
        val metaPath = new Path(new Path(root, typeName), metaFileName)
        val meta = new FileMetadata(fs, metaPath, conf)
        if (!fs.exists(metaPath)) {
          meta.addPartitions(listStorageFiles(typeName))
        }
        val end = System.currentTimeMillis()
        logger.info(s"Loaded metadata in ${end-start}ms")
        meta
      }
    })

  // TODO we don't necessarily want the s3 bucket path to exist...but need to verify we can write
  private val featureTypes: mutable.HashMap[String, SimpleFeatureType] = {
    val m = mutable.HashMap.empty[String, SimpleFeatureType]
    if (fs.exists(root)) {
      val types = fs.listStatus(root).filter(_.isDirectory)
      val schemaFiles = types.map { t => new Path(t.getPath, schemaFile) }
      schemaFiles.map { s =>
        Try {
          val in = fs.open(s)
          val sftConf = try {
             ConfigFactory.parseString(IOUtils.toString(in))
          } finally {
            in.close()
          }
          SimpleFeatureTypes.createType(sftConf)
        }
      }.collect {
        case Success(s) => m += s.getTypeName -> s
        case Failure(ex) => logger.error("Error processing schema file", ex)
      }
    }
    m
  }

  override def listFeatureTypes: util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    featureTypes.values.toList
  }

  override def getFeatureType(name: String): SimpleFeatureType = featureTypes(name)

  override def listPartitions(typeName: String): util.List[Partition] = {
    import scala.collection.JavaConversions._
    metadata(typeName).getPartitions.map(getPartition)
  }

  // TODO ask the parition manager the geometry is fully covered?
  override def getPartitionReader(typeName: String, q: Query, partition: Partition): FileSystemPartitionIterator = {
    val sft = featureTypes(typeName)

    import org.locationtech.geomesa.index.conf.QueryHints._
    QueryPlanner.setQueryTransforms(q, sft)

    val transformSft = q.getHints.getTransformSchema.getOrElse(sft)

    // TODO: push down predicates and partition pruning
    // TODO ensure that transforms are pushed to the ColumnIO in parquet.
    // TODO: Push down full filter that can't be managed
    val fc = new FilterConverter(transformSft).convert(q.getFilter)
    val parquetFilter =
      fc._1
        .map(FilterCompat.get)
        .getOrElse(FilterCompat.NOOP)

    logger.info(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")

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

  override def getWriter(featureType: String, partition: Partition): FileSystemWriter = {
    new FileSystemWriter {
      private val sft      = featureTypes(featureType)
      private val leaf     = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft).isLeafStorage
      private val dataPath = StorageUtils.nextFile(fs, root, featureType, partition.getName, leaf, dataFileExtention)

      private val sftConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.setSft(sft, c)
        if (conf.get("parquet.compression") == null) {
          conf.set("parquet.compression", CompressionCodecName.SNAPPY.name())
        }
        c
      }

      private val writer = SimpleFeatureParquetWriter.builder(dataPath, sftConf).build()

      override def write(f: SimpleFeature): Unit = writer.write(f)

      override def flush(): Unit = {}

      override def close(): Unit = CloseQuietly(writer)
    }
  }

  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit = {
    org.locationtech.geomesa.fs.storage.common.PartitionScheme.addToSft(sft, partitionScheme)
    val path = new Path(root, sft.getTypeName)
    fs.mkdirs(path)
    val encoded = SimpleFeatureTypes.toConfigString(sft, includeUserData = true, concise = false, includePrefix = false)
    val out = fs.create(new Path(path, schemaFile))
    out.writeBytes(encoded)
    out.hflush()
    out.hsync()
    out.close()
    featureTypes.put(sft.getTypeName, sft)
  }

  override def getFileSystemRoot(typeName: String): URI = root.toUri

  override def getPartitionScheme(typeName: String): PartitionScheme = {
    val sft = featureTypes(typeName)
    val conf = sft.getUserData.get(org.locationtech.geomesa.fs.storage.common.PartitionScheme.PartitionSchemeKey).asInstanceOf[String]
    org.locationtech.geomesa.fs.storage.common.PartitionScheme(sft, ConfigFactory.parseString(conf))
  }

  override def getPartition(name: String): Partition = new StoragePartition(name)

  private def listStorageFiles(typeName: String): util.Map[String, util.List[String]] = {
    val scheme = getPartitionScheme(typeName)
    val partitions =
      StorageUtils.buildPartitionList(root, fs, typeName, scheme, StorageUtils.SequenceLength, dataFileExtention)
        .map(getPartition)
    import scala.collection.JavaConverters._
    partitions.map { p =>
      val files = StorageUtils.listFiles(fs, root, typeName, p, scheme.isLeafStorage, dataFileExtention).map(_.getName).asJava
      p.getName -> files
    }.toMap.asJava
  }

  override def getPaths(typeName: String, partition: Partition): java.util.List[URI] = {
    val scheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(featureTypes(typeName))
    val baseDir = if (scheme.isLeafStorage) {
      StorageUtils.partitionPath(root, typeName, partition.getName).getParent
    } else {
      StorageUtils.partitionPath(root, typeName, partition.getName)
    }
    import scala.collection.JavaConversions._
    val files = metadata(typeName).getFiles(partition.getName)
    files.map(new Path(baseDir, _)).map(_.toUri)
  }

  override def getMetadata(typeName: String): Metadata = metadata(typeName)

  override def updateMetadata(typeName: String): Unit = metadata(typeName).addPartitions(listStorageFiles(typeName))

}

object ParquetFileSystemStorage {
  val MetadataCache: Cache[(Path, String), Metadata] = CacheBuilder.newBuilder().build[(Path, String), Metadata]()
}
