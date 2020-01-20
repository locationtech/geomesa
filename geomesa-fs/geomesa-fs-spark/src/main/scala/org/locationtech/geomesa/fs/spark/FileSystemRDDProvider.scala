/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreFactory}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{StorageFileAction, StorageFilePath}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration.SimpleFeatureAction
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.storage.orc.jobs.{OrcSimpleFeatureActionInputFormat, OrcSimpleFeatureInputFormat}
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.parquet.jobs.{ParquetSimpleFeatureActionInputFormat, ParquetSimpleFeatureInputFormat}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class FileSystemRDDProvider extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: java.util.Map[String, _ <: Serializable]): Boolean =
    FileSystemDataStoreFactory.canProcess(params)

  override def rdd(
      conf: Configuration,
      sc: SparkContext,
      params: Map[String, String],
      query: Query): SpatialRDD = {
    WithStore[FileSystemDataStore](params) { ds =>
      val sft = ds.getSchema(query.getTypeName)
      val storage = ds.storage(query.getTypeName)

      def runQuery(filter: Filter, paths: Seq[StorageFilePath], modifications: Boolean): RDD[SimpleFeature] = {
        // note: file input format requires a job object, but conf gets copied in job object creation,
        // so we have to copy the file paths back out
        val job = Job.getInstance(conf)

        // note: we have to copy all the conf twice?
        FileInputFormat.setInputPaths(job, paths.map(_.path): _*)
        conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

        // configure the input format for the storage type
        // we have two input formats for each, depending if we need to do a reduce step or not
        val (base, action) = if (storage.metadata.encoding == OrcFileSystemStorage.Encoding) {
          OrcSimpleFeatureInputFormat.configure(conf, sft, query.getFilter, query.getPropertyNames)
          (classOf[OrcSimpleFeatureInputFormat], classOf[OrcSimpleFeatureActionInputFormat])
        } else if (storage.metadata.encoding == ParquetFileSystemStorage.Encoding) {
          ParquetSimpleFeatureInputFormat.configure(conf, sft, query)
          (classOf[ParquetSimpleFeatureInputFormat], classOf[ParquetSimpleFeatureActionInputFormat])
        } else {
          throw new NotImplementedError(s"Not implemented for encoding '${storage.metadata.encoding}'")
        }

        if (modifications) {
          StorageConfiguration.setPathActions(conf, paths)
          val rdd = sc.newAPIHadoopRDD(conf, action, classOf[SimpleFeatureAction], classOf[SimpleFeature])
          // group updates by feature ID, then take the most recent
          rdd.groupBy(_._1.id).flatMap { case (_, group) =>
            val (action, sf) = group.minBy(_._1)
            if (action.action == StorageFileAction.Delete) { None } else { Some(sf) }
          }
        } else {
          sc.newAPIHadoopRDD(conf, base, classOf[Void], classOf[SimpleFeature]).map(_._2)
        }
      }

      // split up the job by the filters required and partitions that require sequential reads
      // if a partition has modifications, it must be read separately to ensure they are handled correctly
      val partitioned = ArrayBuffer.empty[(String, Filter, Seq[StorageFilePath], Boolean)]

      storage.getPartitionFilters(query.getFilter).foreach { fp =>
        val defaults = ListBuffer.empty[StorageFilePath]
        val defaultPartitions = ListBuffer.empty[String]
        fp.partitions.foreach { p =>
          val files = storage.getFilePaths(p)
          if (files.nonEmpty) {
            if (files.forall(_.file.action == StorageFileAction.Append)) {
              defaults ++= files
              defaultPartitions += p
            } else {
              logger.warn(s"Found modifications for partition '$p': " +
                  "compact the partition to improve read performance")
              partitioned += ((p, fp.filter, files, true))
            }
          }
        }
        if (defaults.nonEmpty) {
          partitioned += ((defaultPartitions.mkString("', '"), fp.filter, defaults, false))
        }
      }

      val rdd = if (partitioned.isEmpty) {
        logger.debug("Reading 0 partitions")
        sc.emptyRDD[SimpleFeature]
      } else {
        val rdds = partitioned.map { case (names, filter, files, modifications) =>
          logger.debug(s"Reading partitions '$names' with ${files.length} files with filter: ${ECQL.toCQL(filter)}")
          runQuery(filter, files, modifications)
        }
        rdds.reduceLeft(_ union _)
      }
      SpatialRDD(rdd, sft)
    }
  }

  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    WithStore[FileSystemDataStore](params) {ds =>
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save. Call createSchema on the DataStore first.")
    }

    rdd.foreachPartition { iter =>
      WithStore[FileSystemDataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
