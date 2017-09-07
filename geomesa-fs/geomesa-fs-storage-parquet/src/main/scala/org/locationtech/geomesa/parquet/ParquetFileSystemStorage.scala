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
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, PartitionScheme, StorageUtils}
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

  override def build(params: util.Map[String, io.Serializable]): ParquetFileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    val conf = new Configuration

    if (params.containsKey(ParquetCompressionOpt)) {
      conf.set(ParquetCompressionOpt, params.get(ParquetCompressionOpt).asInstanceOf[String])
    } else if (System.getProperty(ParquetCompressionOpt) != null) {
      conf.set(ParquetCompressionOpt, System.getProperty(ParquetCompressionOpt))
    }

    conf.set("parquet.filter.dictionary.enabled", "true")
    new ParquetFileSystemStorage(root, root.getFileSystem(conf), conf, params)
  }
}

/**
  *
  * @param root the root of this file system for a specifid SimpleFeatureType
  * @param fs filesystem
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

  // TODO ask the partition manager the geometry is fully covered?
  override def getPartitionReader(sft: SimpleFeatureType, q: Query, partition: String): FileSystemPartitionIterator = {

    import org.locationtech.geomesa.index.conf.QueryHints._
    import scala.collection.JavaConversions._

    // parquetSft has all the fields needed for filtering and return, returnSft just has those needed for return
    val (parquetSft, returnSft) = q.getHints.getTransformSchema match {
      case None => (sft, sft)
      case Some(tsft) =>
        val transforms = tsft.getAttributeDescriptors.map(_.getLocalName)
        val filters = FilterHelper.propertyNames(q.getFilter, sft).filterNot(transforms.contains)
        if (filters.isEmpty) {
          (tsft, tsft)
        } else {
          val builder = new SimpleFeatureTypeBuilder()
          builder.init(tsft)
          filters.foreach(f => builder.add(sft.getDescriptor(f)))
          val psft = builder.buildFeatureType()
          (psft, tsft)
        }
    }

    // TODO GEOMESA-1954 move this filter conversion higher up in the chain
    val (fc, residualFilter) = new FilterConverter(parquetSft).convert(q.getFilter)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)

    logger.debug(s"Parquet filter: $parquetFilter and modified gt filter $residualFilter")

    val transform = if (parquetSft.eq(returnSft)) { None } else { Some(returnSft) }

    val paths = getPaths(sft.getTypeName, partition).toIterator.map(new Path(_)).filter(fs.exists)

    val iters = paths.map { path =>
      // WARNING it is important to create a new conf per query
      // because we communicate the transform SFT set here
      // with the init() method on SimpleFeatureReadSupport via
      // the parquet api. Thus we need to deep copy conf objects
      // It may be possibly to move this high up the chain as well
      // TODO consider this with GEOMESA-1954 but we need to test it well
      val support = new SimpleFeatureReadSupport
      val queryConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.setSft(parquetSft, c)
        c
      }

      val builder = ParquetReader.builder[SimpleFeature](support, path)
        .withFilter(parquetFilter)
        .withConf(queryConf)

      transform match {
        case None => new FilteringIterator(partition, builder, residualFilter)
        case Some(tsft) => new FilteringTransformIterator(partition, builder, residualFilter, parquetSft, tsft)
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

  override def getPaths(typeName: String, partition: String): java.util.List[URI] = {
    val scheme = metadata(typeName).getPartitionScheme
    val baseDir = if (scheme.isLeafStorage) {
      StorageUtils.partitionPath(root, typeName, partition).getParent
    } else {
      StorageUtils.partitionPath(root, typeName, partition)
    }
    import scala.collection.JavaConversions._
    val files = metadata(typeName).getFiles(partition)
    files.map(new Path(baseDir, _).toUri)
  }

  override def getMetadata(typeName: String): Metadata = metadata(typeName)

  override def updateMetadata(typeName: String): Unit = {
    val s = System.currentTimeMillis
    val scheme = metadata(typeName).getPartitionScheme
    val parts = StorageUtils.partitionsAndFiles(root, fs, typeName, scheme, StorageUtils.SequenceLength, FileExtension)
    metadata(typeName).addPartitions(parts)
    val e = System.currentTimeMillis
    logger.info(s"Metadata Update took in ${e-s}ms.")
  }

}

object ParquetFileSystemStorage {
  val ParquetEncoding  = "parquet"
  val FileExtension    = "parquet"
  val MetadataFileName = "metadata.json"

  val ParquetCompressionOpt = "parquet.compression"

  val MetadataCache: Cache[(Path, String), Metadata] = CacheBuilder.newBuilder().build[(Path, String), Metadata]()
}
