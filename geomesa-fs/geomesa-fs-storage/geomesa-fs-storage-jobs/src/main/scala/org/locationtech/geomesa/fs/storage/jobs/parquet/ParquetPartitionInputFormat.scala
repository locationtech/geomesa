/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.jobs.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.iceberg.DataFile
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.core.{CloseableFeatureIterator, FileSystemContext, FileSystemStorage, FileSystemStorageFactory, Partition, PartitionScheme, StorageMetadata, StorageCatalog}
import org.locationtech.geomesa.fs.storage.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.jobs.parquet.ParquetPartitionInputFormat.{PartitionInputSplit, PartitionRecordReader}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{DataInput, DataOutput}
import java.net.URI

/**
  * An Input format that creates splits based on FSDS Partitions. This is used for compaction, when we want a single
  * split per partition. Otherwise, use OrcSimpleFeatureInputFormat/ParquetSimpleFeatureInputFormat as those are
  * more efficient
  */
class ParquetPartitionInputFormat extends InputFormat[Void, SimpleFeature] {

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val hadoopConf = context.getConfiguration
    val conf = {
      val builder = Map.newBuilder[String, String]
      hadoopConf.forEach(e => builder += e.getKey -> hadoopConf.get(e.getKey)) // use .get to resolve envs
      builder.result()
    }

    val root = StorageConfiguration.getRootPath(hadoopConf)
    val fileSize = StorageConfiguration.getTargetFileSize(hadoopConf)
    val typeName = StorageConfiguration.getSftName(hadoopConf)

    val fsc = FileSystemContext.create(root, conf)
    val catalog = StorageCatalog(fsc)
    val factory = new ParquetFileSystemStorageFactory()
    WithClose(factory.apply(fsc, catalog.load(typeName))) { storage =>
      val sizeCheck = fileSize.orElse(storage.sizer.targetSize).map(t => (p: URI) => storage.sizer.fileIsSized(p, t))
      val splits = StorageConfiguration.getPartitions(hadoopConf).map { partition =>
        var size = 0L
        val files = storage.metadata.getFiles(partition).filter { f =>
          if (sizeCheck.exists(_.apply(fsc.root.resolve(f.location())))) { false } else {
            size += storage.fs.size(fsc.root.resolve(f.location()))
            true
          }
        }
        new PartitionInputSplit(partition.toString, files, size)
      }
      java.util.Arrays.asList(splits: _*)
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] = {
    val psplit = split.asInstanceOf[PartitionInputSplit]
    // Check if files are available (not deserialized) or if we need to use locations
    if (psplit.getFiles != null) {
      new PartitionRecordReader(Left(psplit.getFiles))
    } else {
      new PartitionRecordReader(Right(psplit.getFileLocations))
    }
  }
}

object ParquetPartitionInputFormat {

  /**
    * InputSplit corresponding to a single FileSystemDataStore PartitionScheme partition
    */
  class PartitionInputSplit extends InputSplit with Writable {

    private var name: String = _
    private var files: Seq[DataFile] = _
    private var length: java.lang.Long = _

    def this(name: String, files: Seq[DataFile], length: Long) = {
      this()
      this.name = name
      this.files = files
      this.length = length
    }

    /**
      * @return the name of this partition
      */
    def getName: String = name

    def getFiles: Seq[DataFile] = files

    override def getLength: Long = length

    // TODO attempt to optimize the locations where this should run in the case of HDFS
    // With S3 this won't really matter
    override def getLocations: Array[String] = Array.empty[String]

    override def write(out: DataOutput): Unit = {
      out.writeUTF(name)
      out.writeLong(length)
      out.writeInt(files.length)
      // Store only the file locations - we'll reconstruct DataFiles later if needed
      files.foreach { file =>
        out.writeUTF(file.location())
        out.writeLong(file.recordCount())
      }
    }

    override def readFields(in: DataInput): Unit = {
      this.name = in.readUTF()
      this.length = in.readLong()
      val count = in.readInt
      // Store file locations temporarily - they'll be resolved to actual DataFiles in the reader
      val locations = Seq.fill(count) {
        val location = in.readUTF()
        val recordCount = in.readLong()
        (location, recordCount)
      }
      // We'll set files to null here and resolve them lazily in the reader
      // This is a bit awkward but avoids needing PartitionSpec at deserialization time
      this.files = null
      this._locations = locations
    }

    private var _locations: Seq[(String, Long)] = _
    def getFileLocations: Seq[(String, Long)] = if (_locations != null) _locations else files.map(f => (f.location(), f.recordCount()))
  }

  class PartitionRecordReader(filesOrLocations: Either[Seq[DataFile], Seq[(String, Long)]]) extends RecordReader[Void, SimpleFeature] {

    private var storage: FileSystemStorage = _
    private var reader: CloseableFeatureIterator = _

    private var curValue: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val hadoopConf = context.getConfiguration
      val conf = {
        val builder = Map.newBuilder[String, String]
        hadoopConf.forEach(e => builder += e.getKey -> e.getValue)
        builder.result()
      }
      val root = StorageConfiguration.getRootPath(hadoopConf)
      val fsc = FileSystemContext.create(root, conf)
      val sft = StorageConfiguration.getSft(hadoopConf)
      val encoding = StorageConfiguration.getEncoding(hadoopConf)

      // Resolve files if we only have locations
      val files = filesOrLocations match {
        case Left(dataFiles) => dataFiles
        case Right(locations) =>
          // Load metadata to reconstruct DataFiles
          val catalog = StorageCatalog(fsc)
          WithClose(catalog.load(sft.getTypeName)) { metadata =>
            locations.map { case (location, _) =>
              // Create DataFile with location - partition will be extracted from file name or metadata
              metadata.createDataFile(location, Partition.None)
            }
          }
      }

      // use a cached metadata impl instead of reloading
      val cached = new StaticMetadata(sft, files)
      storage = FileSystemStorageFactory(encoding).apply(fsc, cached)
      reader = storage.getReader(new Query("", Filter.INCLUDE), threads = math.min(8, files.size))
    }

    // TODO look at how the ParquetInputFormat provides progress and utilize something similar
    override def getProgress: Float = 0.0f

    override def nextKeyValue(): Boolean = {
      if (reader.hasNext) {
        curValue = reader.next()
        true
      } else {
        curValue = null
        false
      }
    }

    override def getCurrentKey: Void = null
    override def getCurrentValue: SimpleFeature = curValue

    override def close(): Unit = {
      if (reader != null) {
        CloseWithLogging(reader)
      }
      if (storage != null) {
        CloseWithLogging(storage)
      }
    }
  }

  private class StaticMetadata(val sft: SimpleFeatureType, files: Seq[DataFile]) extends StorageMetadata with LazyLogging {
    override def `type`: String = "static"
    override def createDataFile(
        filePath: String,
        partition: Partition,
        content: org.apache.iceberg.FileContent = org.apache.iceberg.FileContent.DATA): DataFile =
      throw new UnsupportedOperationException()
    override def getFiles(): Seq[DataFile] = files
    override def getFiles(partition: Partition): Seq[DataFile] = {
      // We don't have partition information easily accessible from DataFile without IcebergMapper
      // For compaction jobs, this typically isn't called with specific partitions
      logger.warn(s"getFiles(partition) called on StaticMetadata - returning all files")
      files
    }
    override def getFiles(filter: Filter): Seq[DataFile] = {
      // note: should only be called with filter.include
      if (filter != Filter.INCLUDE) {
        logger.warn(s"Unexpected filter: $filter")
      }
      files
    }
    override def addFiles(files: Seq[DataFile]): Unit = throw new UnsupportedOperationException()
    override def removeFile(file: DataFile): Unit = throw new UnsupportedOperationException()
    override def replaceFiles(existing: Seq[DataFile], replacements: Seq[DataFile]): Unit =
      throw new UnsupportedOperationException()
    override def schemes: Set[PartitionScheme] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }
}
