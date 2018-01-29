/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemReader
import org.locationtech.geomesa.fs.storage.common.jobs.PartitionInputFormat.PartitionInputSplit
import org.locationtech.geomesa.fs.storage.common.{FileSystemStorageFactory, StorageUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * An Input format that creates splits based on FSDS Partitions
  */
class PartitionInputFormat extends InputFormat[Void, SimpleFeature] {

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val typeName = StorageConfiguration.getSft(context.getConfiguration).getTypeName
    val encoding = StorageConfiguration.getEncoding(context.getConfiguration)
    val partitions = StorageConfiguration.getPartitions(context.getConfiguration)

    val rootPath = new Path(StorageConfiguration.getPath(context.getConfiguration))
    val fs = rootPath.getFileSystem(context.getConfiguration)

    val splits = partitions.map { p =>
      val pp = StorageUtils.partitionPath(rootPath, typeName, p)
      val size = StorageUtils.listFileStatuses(fs, pp, encoding).map(_.getLen).sum
      new PartitionInputSplit(p, size)
    }

    java.util.Arrays.asList(splits: _*)
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] = {

    val partitionInputSplit = split.asInstanceOf[PartitionInputSplit]

    new RecordReader[Void, SimpleFeature] {
      private var sft: SimpleFeatureType = _
      private var reader: FileSystemReader = _

      private var curValue: SimpleFeature = _

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
        import scala.collection.JavaConversions._

        sft = StorageConfiguration.getSft(context.getConfiguration)

        val path = StorageConfiguration.getPath(context.getConfiguration)
        val encoding = StorageConfiguration.getEncoding(context.getConfiguration)
        val params = Map(
          FileSystemStorageFactory.PathParam.getName -> path,
          FileSystemStorageFactory.EncodingParam.getName -> encoding
        )
        val query = new Query(sft.getTypeName, Filter.INCLUDE)

        val storage = FileSystemStorageFactory.getFileSystemStorage(params)
        reader = storage.getReader(sft.getTypeName, Seq(partitionInputSplit.getName), query)
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

      override def close(): Unit = reader.close()
    }
  }
}

object PartitionInputFormat {

  /**
    * InputSplit corresponding to a single FileSystemDataStore PartitionScheme partition
    */
  class PartitionInputSplit(private var name: String, private var length: Long) extends InputSplit with Writable {

    def this() = this(null, 0L)

    /**
      * @return the name of this partition
      */
    def getName: String = name

    override def getLength: Long = length

    // TODO attempt to optimize the locations where this should run in the case of HDFS
    // With S3 this won't really matter
    override def getLocations: Array[String] = Array.empty[String]

    override def write(out: DataOutput): Unit = {
      out.writeUTF(name)
      out.writeLong(length)
    }

    override def readFields(in: DataInput): Unit = {
      this.name = in.readUTF()
      this.length = in.readLong()
    }
  }
}
