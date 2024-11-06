/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.SizeableFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
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

    val metadata = StorageMetadataFactory.load(fsc).getOrElse {
      throw new IllegalArgumentException(s"No storage defined under path '$root'")
    }
    WithClose(metadata) { meta =>
      WithClose(FileSystemStorageFactory(fsc, meta)) { storage =>
        val sizeable = Option(storage).collect { case s: SizeableFileSystemStorage => s }
        val sizeCheck = sizeable.flatMap(s => s.targetSize(fileSize).map(t => (p: Path) => s.fileIsSized(p, t)))
        val splits = StorageConfiguration.getPartitions(conf).map { partition =>
          var size = 0L
          val files = storage.getFilePaths(partition).filter { f =>
            if (sizeCheck.exists(_.apply(f.path))) { false } else {
              size += PathCache.status(fsc.fs, f.path).getLen
              true
            }
          }
          new PartitionInputSplit(partition, files.map(_.file), size)
        }
        java.util.Arrays.asList(splits: _*)
      }
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] = {
    val psplit = split.asInstanceOf[PartitionInputSplit]
    new PartitionRecordReader(psplit.getName, psplit.getFiles)
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
      files.foreach { case StorageFile(file, ts, action, _ , _) =>
        out.writeUTF(file)
        out.writeLong(ts)
        out.writeUTF(action.toString)
      }
    }

    override def readFields(in: DataInput): Unit = {
      this.name = in.readUTF()
      this.length = in.readLong()
      this.files = Seq.fill(in.readInt) {
        StorageFile(in.readUTF(), in.readLong, StorageFileAction.withName(in.readUTF()))
      }
    }
  }

  class PartitionRecordReader(partition: String, files: Seq[StorageFile]) extends RecordReader[Void, SimpleFeature] {

    private var storage: FileSystemStorage = _
    private var reader: CloseableFeatureIterator = _

    private var curValue: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val conf = context.getConfiguration
      val root = StorageConfiguration.getRootPath(conf)
      val fsc = FileSystemContext(root, conf)
      val metadata = StorageMetadataFactory.load(fsc).getOrElse {
        throw new IllegalArgumentException(s"No storage defined under path '$root'")
      }
      // use a cached metadata impl instead of reloading
      val data = PartitionMetadata(partition, files, None, 0L)
      val cached = new CachedMetadata(metadata.sft, metadata.encoding, metadata.scheme, metadata.leafStorage, data)
      storage = FileSystemStorageFactory(fsc, cached)
      reader = storage.getReader(new Query("", Filter.INCLUDE), Option(partition))
      metadata.close()
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

    override def close(): Unit = CloseWithLogging(reader, storage)
  }

  class CachedMetadata(
      val sft: SimpleFeatureType,
      val encoding: String,
      val scheme: PartitionScheme,
      val leafStorage: Boolean,
      partition: PartitionMetadata
  ) extends StorageMetadata {
    override def getPartition(name: String): Option[PartitionMetadata] =
      if (partition.name == name) { Some(partition) } else { None }
    override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] =
      if (prefix.forall(partition.name.startsWith)) { Seq(partition) } else { Seq.empty }
    override def addPartition(partition: PartitionMetadata): Unit = throw new NotImplementedError()
    override def removePartition(partition: PartitionMetadata): Unit = throw new NotImplementedError()
    override def setPartitions(partitions: Seq[PartitionMetadata]): Unit = throw new NotImplementedError()
    override def compact(partition: Option[String], fileSize: Option[Long], threads: Int): Unit =
      throw new NotImplementedError()
    override def invalidate(): Unit = {}
    override def close(): Unit = {}
  }
}
