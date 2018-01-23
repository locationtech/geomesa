/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputFormat, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

abstract class AbstractFileSystemRDDProvider extends SpatialRDDProvider with LazyLogging {

  protected def inputFormat: Class[_ <: InputFormat[Void, SimpleFeature]]

  protected def configure(conf: Configuration, sft: SimpleFeatureType, query: Query): Unit

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    import scala.collection.JavaConversions._

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[FileSystemDataStore]
    try {
      val sft = ds.getSchema(query.getTypeName)

      val storage = ds.storage
      val inputPaths = storage.getPartitions(sft.getTypeName, query).flatMap { p =>
        storage.getPaths(sft.getTypeName, p).map(new Path(_))
      }

      // note: file input format requires a job object, but conf gets copied in job object creation,
      // so we have to copy the file paths back out
      val job = Job.getInstance(conf)

      // Note we have to copy all the conf twice?
      FileInputFormat.setInputPaths(job, inputPaths: _*)
      conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

      configure(conf, sft, query)

      val rdd = sc.newAPIHadoopRDD(conf, inputFormat, classOf[Void], classOf[SimpleFeature])
      SpatialRDD(rdd.map(_._2), sft)
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
}
