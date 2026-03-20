/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile, StorageFileAction, StorageFileFilter}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.SizeableFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.metadata.StorageMetadataCatalog
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.tools.compact.PartitionInputFormat.{PartitionInputSplit, PartitionRecordReader}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{DataInput, DataOutput}

/**
  * An Input format that creates splits based on FSDS Partitions. This is used for compaction, when we want a single
  * split per partition. Otherwise, use OrcSimpleFeatureInputFormat/ParquetSimpleFeatureInputFormat as those are
  * more efficient
  */
class PartitionInputFormat extends InputFormat[Void, SimpleFeature] {

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = context.getConfiguration

    val root = StorageConfiguration.getRootPath(conf)
    val fsc = FileSystemContext(root, conf)
    val fileSize = StorageConfiguration.getTargetFileSize(conf)
    val metadataType = StorageConfiguration.getMetadataType(conf)
    val metadataConfig = StorageConfiguration.getMetadataConfig(conf)
    val typeName = StorageConfiguration.getSftName(conf)
    val encoding = StorageConfiguration.getEncoding(conf)

    WithClose(StorageMetadataCatalog(fsc, metadataType, metadataConfig).load(typeName)) { meta =>
      WithClose(FileSystemStorageFactory(fsc, meta, encoding)) { storage =>
        val sizeable = Option(storage).collect { case s: SizeableFileSystemStorage => s }
        val sizeCheck = sizeable.flatMap(s => s.targetSize(fileSize).map(t => (p: Path) => s.fileIsSized(p, t)))
        val splits = StorageConfiguration.getPartitions(conf).map { partition =>
          var size = 0L
          val files = storage.metadata.getFiles(partition).filter { f =>
            if (sizeCheck.exists(_.apply(new Path(fsc.root, f.file)))) { false } else {
              size += PathCache.status(fsc.fs, new Path(fsc.root, f.file)).getLen
              true
            }
          }
          new PartitionInputSplit(partition.encoded, files, size)
        }
        java.util.Arrays.asList(splits: _*)
      }
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] = {
    val psplit = split.asInstanceOf[PartitionInputSplit]
    new PartitionRecordReader(psplit.getFiles)
  }
}

object PartitionInputFormat {

  /**
    * InputSplit corresponding to a single FileSystemDataStore PartitionScheme partition
    */
  class PartitionInputSplit extends InputSplit with Writable {

    private var name: String = _
    private var files: Seq[StorageFile] = _
    private var length: java.lang.Long = _

    def this(name: String, files: Seq[StorageFile], length: Long) = {
      this()
      this.name = name
      this.files = files
      this.length = length
    }

    /**
      * @return the name of this partition
      */
    def getName: String = name

    def getFiles: Seq[StorageFile] = files

    override def getLength: Long = length

    // TODO attempt to optimize the locations where this should run in the case of HDFS
    // With S3 this won't really matter
    override def getLocations: Array[String] = Array.empty[String]

    override def write(out: DataOutput): Unit = {
      out.writeUTF(name)
      out.writeLong(length)
      out.writeInt(files.length)
      files.foreach { file =>
        // TODO bounds, sort
        out.writeUTF(file.file)
        out.writeUTF(file.partition.encoded)
        out.writeLong(file.count)
        out.writeUTF(file.action.toString)
        out.writeLong(file.timestamp)
      }
    }

    override def readFields(in: DataInput): Unit = {
      this.name = in.readUTF()
      this.length = in.readLong()
      this.files = Seq.fill(in.readInt) {
        StorageFile(in.readUTF(), Partition(in.readUTF()), in.readLong, StorageFileAction.withName(in.readUTF()), Seq.empty, Seq.empty, Seq.empty, in.readLong())
      }
    }
  }

  class PartitionRecordReader(files: Seq[StorageFile]) extends RecordReader[Void, SimpleFeature] {

    private var storage: FileSystemStorage = _
    private var reader: CloseableFeatureIterator = _

    private var curValue: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val conf = context.getConfiguration
      val root = StorageConfiguration.getRootPath(conf)
      val fsc = FileSystemContext(root, conf)
      val sft = StorageConfiguration.getSft(conf)
      val encoding = StorageConfiguration.getEncoding(conf)

      // use a cached metadata impl instead of reloading
      val cached = new StaticMetadata(sft, files)
      storage = FileSystemStorageFactory(fsc, cached, encoding)
      reader = storage.getReader(new Query("", Filter.INCLUDE))
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

  private class StaticMetadata(val sft: SimpleFeatureType, files: Seq[StorageFile]) extends StorageMetadata with LazyLogging {
    override def `type`: String = "static"
    override def getFiles(): Seq[StorageFile] = files
    override def getFiles(partition: Partition): Seq[StorageFile] = files.filter(_.partition == partition)
    override def getFiles(filter: Filter): Seq[StorageFileFilter] = {
      // note: should only be called with filter.include
      if (filter != Filter.INCLUDE) {
        logger.warn(s"Unexpected filter: $filter")
      }
      files.map(StorageFileFilter(_, None))
    }
    override def addFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
    override def removeFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
    override def schemes: Set[PartitionScheme] = throw new UnsupportedOperationException()
    override def close(): Unit = {}
  }
}
