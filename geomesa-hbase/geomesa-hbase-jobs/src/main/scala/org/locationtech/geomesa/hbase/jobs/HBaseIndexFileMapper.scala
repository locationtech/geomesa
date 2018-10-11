/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Converts simple features to HBase mutations
  */
class HBaseIndexFileMapper extends Mapper[Writable, SimpleFeature, ImmutableBytesWritable, Put] with LazyLogging {

  private var ds: HBaseDataStore = _
  private var sft: SimpleFeatureType = _
  private var writer: HBaseFeature => Seq[Mutation] = _
  private var wrapper: SimpleFeature => HBaseFeature = _

  private var features: Counter = _
  private var entries: Counter = _
  private var failed: Counter = _

  private val bytes = new ImmutableBytesWritable

  override def setup(context: HBaseIndexFileMapper.MapContext): Unit = {
    import scala.collection.JavaConversions._
    val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
    require(ds != null, "Could not find data store - check your configuration and hbase-site.xml")
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureTypeOut(context.getConfiguration))
    require(sft != null, "Could not find schema - check your configuration")
    val index = GeoMesaConfigurator.getIndicesOut(context.getConfiguration) match {
      case Some(Seq(idx)) => ds.manager.index(idx)
      case _ => throw new IllegalArgumentException("Could not find write index - check your configuration")
    }
    writer = index.writer(sft, ds)
    wrapper = HBaseFeature.wrapper(sft)

    features = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
    entries = context.getCounter(GeoMesaOutputFormat.Counters.Group, "entries")
    failed = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)
  }

  override def cleanup(context: HBaseIndexFileMapper.MapContext): Unit = ds.dispose()

  override def map(key: Writable, value: SimpleFeature, context: HBaseIndexFileMapper.MapContext): Unit = {
    try {
      val feature = wrapper(value)
      writer.apply(feature).asInstanceOf[Seq[Put]].foreach { put =>
        bytes.set(put.getRow)
        context.write(bytes, put)
        entries.increment(1L)
      }
      features.increment(1L)
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error writing feature ${Option(value).orNull}", e)
        failed.increment(1L)
    }
  }
}

object HBaseIndexFileMapper {

  type MapContext = Mapper[Writable, SimpleFeature, ImmutableBytesWritable, Put]#Context

  /**
    * Sets mapper class, reducer class, output format and associated options
    *
    * @param job job
    * @param params data store params for output data
    * @param typeName feature type name to write (schema must exist already)
    * @param index index table to write
    * @param output output path for HFiles
    */
  def configure(job: Job,
                params: Map[String, String],
                typeName: String,
                index: String,
                output: Path): Unit = {
    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
    require(ds != null, s"Could not find data store with provided parameters ${params.mkString(",")}")
    try {
      val sft = ds.getSchema(typeName)
      require(sft != null, s"Schema $typeName does not exist, please create it first")
      require(!TablePartition.partitioned(sft), "Writing to partitioned tables is not currently supported")
      val idx = ds.manager.index(index)
      val tableName = TableName.valueOf(idx.getTableNames(sft, ds, None).head)
      val table = ds.connection.getTable(tableName)

      GeoMesaConfigurator.setDataStoreOutParams(job.getConfiguration, params)
      GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, typeName)
      GeoMesaConfigurator.setIndicesOut(job.getConfiguration, Seq(idx))
      FileOutputFormat.setOutputPath(job, output)

      // this defaults to /user/<user>/hbase-staging, which generally doesn't exist...
      job.getConfiguration.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
        s"${System.getProperty("java.io.tmpdir")}/hbase-staging")
      // this requires a connection to hbase, which we don't always have
      // TODO allow this as an option
      job.getConfiguration.set(HFileOutputFormat2.LOCALITY_SENSITIVE_CONF_KEY, "false")

      job.setMapperClass(classOf[HBaseIndexFileMapper])
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[Put])

      // sets reducer, output classes, num-reducers, and hbase config
      // override the libjars hbase tries to set, as they end up conflicting with the ones we set
      val libjars = job.getConfiguration.get("tmpjars")
      HFileOutputFormat2.configureIncrementalLoad(job, table, ds.connection.getRegionLocator(tableName))
      job.getConfiguration.set("tmpjars", libjars)
    } finally {
      ds.dispose()
    }
  }
}
