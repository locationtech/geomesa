/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.fs.{FileSystemDataStore, FsQueryPlanning}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.parquet.{FilterConverter, ParquetFileSystemStorageFactory, SFParquetInputFormat, SimpleFeatureReadSupport}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.SimpleFeature

class ParquetFileSystemRDD extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    new ParquetFileSystemStorageFactory().canProcess(params)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {

    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[FileSystemDataStore]
    val origSft = ds.getSchema(query.getTypeName)
    import org.locationtech.geomesa.index.conf.QueryHints._

    QueryPlanner.setQueryTransforms(query, origSft)
    val sft = query.getHints.getTransformSchema.getOrElse(origSft)


    val storage = ds.storage
    val inputPaths = FsQueryPlanning.getPartitionsForQuery(storage, origSft, query).flatMap { p =>
      storage.getPaths(sft.getTypeName, p).map(new Path(_))
    }

    // note: file input format requires a job object, but conf gets copied in job object creation,
    // so we have to copy the file paths back out
    val job = Job.getInstance(conf)

    // Note we have to copy all the conf twice?
    FileInputFormat.setInputPaths(job, inputPaths: _*)
    conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

    // Note we have to copy all the conf twice?
    SimpleFeatureReadSupport.setSft(sft, job.getConfiguration)
    SimpleFeatureReadSupport.setSft(sft, conf)

    // Pushdown Parquet Predicates
    val (parquetFilter, modifiedGT) = new FilterConverter(origSft).convert(query.getFilter)
    parquetFilter.foreach { f =>
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, f)
      ParquetInputFormat.setFilterPredicate(conf, f)
    }

    // Need this for query planning
    conf.set("parquet.filter.dictionary.enabled", "true")
    job.getConfiguration.set("parquet.filter.dictionary.enabled", "true")

    // Now set the modified geotools filter
    SFParquetInputFormat.setGeoToolsFilter(job.getConfiguration, modifiedGT)
    conf.set(SFParquetInputFormat.GeoToolsFilterKey, job.getConfiguration.get(SFParquetInputFormat.GeoToolsFilterKey))

    // Note we have to copy all the conf twice?
    ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, job.getConfiguration.get(ParquetInputFormat.READ_SUPPORT_CLASS))

    val rdd = sc.newAPIHadoopRDD(conf, classOf[SFParquetInputFormat], classOf[Void], classOf[SimpleFeature])
    SpatialRDD(rdd.map(_._2), sft)
  }

  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[FileSystemDataStore]
    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
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
}
