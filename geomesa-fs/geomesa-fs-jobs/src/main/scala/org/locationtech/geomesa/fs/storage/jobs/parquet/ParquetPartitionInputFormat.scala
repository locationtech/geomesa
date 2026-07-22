/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.jobs.parquet

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.iceberg.DataFile
import org.apache.iceberg.expressions.Expressions
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergParquetScan
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.fs.storage.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.jobs.parquet.ParquetPartitionInputFormat.{PartitionInputSplit, PartitionRecordReader}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{DataInput, DataOutput}

/**
  * An Input format that creates splits based on FSDS Partitions. This is used for compaction, when we want a single
  * split per partition. Otherwise, use ParquetSimpleFeatureInputFormat as that is more efficient
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
    WithClose(StorageCatalog(fsc)) { catalog =>
      WithClose(catalog.load(typeName)) { storage =>
        val sizeCheck = fileSize.orElse(storage.sizer.targetSize).map(t => (f: DataFile) => storage.sizer.fileIsSized(f, t))
        val splits = StorageConfiguration.getPartitions(hadoopConf).map { partition =>
          var size = 0L
          val files = storage.metadata.files().forPartition(partition).scan().filter { f =>
            if (sizeCheck.exists(_.apply(f))) { false } else {
              size += f.fileSizeInBytes()
              true
            }
          }
          new PartitionInputSplit(partition.toString, files.map(_.location()), size)
        }
        java.util.Arrays.asList(splits: _*)
      }
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] =
    new PartitionRecordReader(split.asInstanceOf[PartitionInputSplit].getFiles)
}

object ParquetPartitionInputFormat {

  /**
    * InputSplit corresponding to a single FileSystemDataStore PartitionScheme partition
    */
  class PartitionInputSplit extends InputSplit with Writable {

    private var name: String = _
    private var files: Seq[String] = _
    private var length: java.lang.Long = _

    def this(name: String, files: Seq[String], length: Long) = {
      this()
      this.name = name
      this.files = files
      this.length = length
    }

    /**
      * @return the name of this partition
      */
    def getName: String = name

    def getFiles: Seq[String] = files

    override def getLength: Long = length

    // TODO attempt to optimize the locations where this should run in the case of HDFS
    // With S3 this won't really matter
    override def getLocations: Array[String] = Array.empty[String]

    override def write(out: DataOutput): Unit = {
      out.writeUTF(name)
      out.writeLong(length)
      out.writeInt(files.length)
      files.foreach(out.writeUTF)
    }

    override def readFields(in: DataInput): Unit = {
      this.name = in.readUTF()
      this.length = in.readLong()
      val count = in.readInt
      this.files = Seq.fill(count)(in.readUTF())
    }
  }

  class PartitionRecordReader(files: Seq[String]) extends RecordReader[Void, SimpleFeature] {

    private var catalog: StorageCatalog = _
    private var storage: FileSystemStorage = _
    private var reader: CloseableIterator[SimpleFeature] = _

    private var curValue: SimpleFeature = _

    private def filterFiles(location: String): Boolean = files.contains(location)

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val hadoopConf = context.getConfiguration
      val conf = {
        val builder = Map.newBuilder[String, String]
        hadoopConf.forEach(e => builder += e.getKey -> e.getValue)
        builder.result()
      }
      val root = StorageConfiguration.getRootPath(hadoopConf)
      val typeName = StorageConfiguration.getSftName(hadoopConf)
      val fsc = FileSystemContext.create(root, conf)
      catalog = StorageCatalog(fsc)
      storage = catalog.load(typeName)

      val readSchema = storage.schema.read(None, Set.empty)
      reader = new IcebergParquetScan(storage.table, readSchema, Expressions.alwaysTrue(), math.min(8, files.size), Some(filterFiles))
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

    override def close(): Unit = CloseWithLogging(Seq(reader, storage, catalog).filter(_ != null))
  }
}
