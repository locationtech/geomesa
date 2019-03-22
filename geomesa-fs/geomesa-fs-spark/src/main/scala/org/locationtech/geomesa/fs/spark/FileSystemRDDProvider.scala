/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputFormat, Job}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreFactory}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcSimpleFeatureInputFormat
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.parquet.jobs.{ParquetSimpleFeatureInputFormat, SimpleFeatureReadSupport}
import org.locationtech.geomesa.parquet.{FilterConverter, ParquetFileSystemStorage}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class FileSystemRDDProvider extends SpatialRDDProvider with LazyLogging {

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    new FileSystemDataStoreFactory().canProcess(params)

  override def rdd(
      conf: Configuration,
      sc: SparkContext,
      params: Map[String, String],
      query: Query): SpatialRDD = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]
    try {
      val sft = ds.getSchema(query.getTypeName)

      val storage = ds.storage(query.getTypeName)

      // split up the job by the filters required for each partition group
      val filters = storage.getPartitionFilters(query.getFilter).flatMap { fp =>
        val paths = fp.partitions.flatMap(storage.getFilePaths)
        if (paths.isEmpty) { Seq.empty } else {
          Seq(fp.filter -> paths)
        }
      }

      def runQuery(filter: Filter, paths: Seq[Path]): RDD[SimpleFeature] = {
        // note: file input format requires a job object, but conf gets copied in job object creation,
        // so we have to copy the file paths back out
        val job = Job.getInstance(conf)

        // Note we have to copy all the conf twice?
        FileInputFormat.setInputPaths(job, paths: _*)
        conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

        val q = new Query(query)
        q.setFilter(filter)

        val inputFormat = storage.metadata.encoding match {
          case OrcFileSystemStorage.Encoding => configureOrc(conf, sft, q)
          case ParquetFileSystemStorage.Encoding => configureParquet(conf, sft, q)
          case e => throw new RuntimeException(s"Not implemented for encoding '$e'")
        }

        sc.newAPIHadoopRDD(conf, inputFormat, classOf[Void], classOf[SimpleFeature]).map(_._2)
      }

      val rdd = if (filters.isEmpty) {
        logger.debug("Reading 0 partitions")
        sc.emptyRDD[SimpleFeature]
      } else {
        val rdds = filters.map { case (filter, partitions) =>
          logger.debug(s"Reading ${partitions.length} partitions with filter: $filter")
          runQuery(filter, partitions)
        }
        rdds.reduceLeft(_ union _)
      }
      SpatialRDD(rdd, sft)
    } finally {
      ds.dispose()
    }
  }

  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[FileSystemDataStore]
    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save. Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[FileSystemDataStore]
      val featureWriter = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, useProvidedFid = true)
          featureWriter.write()
        }
      } finally {
        IOUtils.closeQuietly(featureWriter)
        ds.dispose()
      }
    }
  }

  private def configureOrc(conf: Configuration,
                           sft: SimpleFeatureType,
                           query: Query): Class[_ <: InputFormat[Void, SimpleFeature]] = {
    OrcSimpleFeatureInputFormat.configure(conf, sft, query.getFilter, query.getPropertyNames)
    classOf[OrcSimpleFeatureInputFormat]
  }

  private def configureParquet(conf: Configuration,
                               sft: SimpleFeatureType,
                               query: Query): Class[_ <: InputFormat[Void, SimpleFeature]] = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    QueryPlanner.setQueryTransforms(query, sft)

    StorageConfiguration.setSft(conf, query.getHints.getTransformSchema.getOrElse(sft))

    // Need this for query planning
    conf.set("parquet.filter.dictionary.enabled", "true")

    // push-down Parquet predicates and remaining gt-filter
    val (parquetFilter, modifiedGT) = new FilterConverter(sft).convert(query.getFilter)
    parquetFilter.foreach(ParquetInputFormat.setFilterPredicate(conf, _))
    ParquetSimpleFeatureInputFormat.setGeoToolsFilter(conf, modifiedGT)

    // @see org.apache.parquet.hadoop.ParquetInputFormat.setReadSupportClass(org.apache.hadoop.mapred.JobConf, java.lang.Class<?>)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[SimpleFeatureReadSupport].getName)

    classOf[ParquetSimpleFeatureInputFormat]
  }
}
