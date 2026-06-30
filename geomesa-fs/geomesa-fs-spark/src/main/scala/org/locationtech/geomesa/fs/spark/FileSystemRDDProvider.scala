/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.iceberg.DataFile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreFactory}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.jobs.parquet.ParquetSimpleFeatureInputFormat
import org.locationtech.geomesa.index.utils.FeatureWriterHelper
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}

class FileSystemRDDProvider extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    FileSystemDataStoreFactory.canProcess(params)

  override def rdd(
      conf: Configuration,
      sc: SparkContext,
      params: Map[String, String],
      query: Query): SpatialRDD = {
    WithStore[FileSystemDataStore](params) { ds =>
      val sft = ds.getSchema(query.getTypeName)
      val storage = ds.storage(query.getTypeName)

      // set s3a configs first, may be needed to set input paths
      storage.context.conf.foreach { case (k, v) => conf.set(k, v) }

      def configureQuery(filter: Filter, paths: Seq[DataFile]): Unit = {
        logger.debug(s"Reading ${paths.length} files with filter: ${ECQL.toCQL(filter)}")
        // note: file input format requires a job object, but conf gets copied in job object creation,
        // so we have to copy the file paths back out
        val job = Job.getInstance(conf)

        // note: we have to copy all the conf twice?
        FileInputFormat.setInputPaths(job, paths.map(p => new Path(S3ObjectStore.s3aUri(storage.context.root.resolve(p.location())))): _*)
        conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))
        val newQuery = new Query(query)
        newQuery.setFilter(filter)
        ParquetSimpleFeatureInputFormat.configure(conf, sft, newQuery)
      }

      def runAppendQuery(filter: Filter, paths: Seq[DataFile]): RDD[SimpleFeature] = {
        configureQuery(filter, paths)
        sc.newAPIHadoopRDD(conf, classOf[ParquetSimpleFeatureInputFormat], classOf[Void], classOf[SimpleFeature]).map(_._2)
      }

      // TODO support modifications
      val files = storage.metadata.files().forFilter(query.getFilter).scan()
      val rdd = if (files.isEmpty) { sc.emptyRDD[SimpleFeature] } else { runAppendQuery(query.getFilter, files) }
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
          val helper = FeatureWriterHelper(writer, useProvidedFids = true)
          iter.foreach(helper.write)
        }
      }
    }
  }
}
