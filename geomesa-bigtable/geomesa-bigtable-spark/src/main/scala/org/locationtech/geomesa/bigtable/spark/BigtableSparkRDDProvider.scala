/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.spark

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.spark.SparkContext
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.bigtable.data.BigtableDataStoreFactory
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore}
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBaseIndexAdapter}
import org.locationtech.geomesa.hbase.jobs.HBaseGeoMesaRecordReader
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.spark.SpatialRDD
import org.locationtech.geomesa.spark.hbase.HBaseSpatialRDDProvider
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class BigtableSparkRDDProvider extends HBaseSpatialRDDProvider {
  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    BigtableDataStoreFactory.canProcess(params)

  override def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          origQuery: Query): SpatialRDD = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[HBaseDataStore]

    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qp = ds.getQueryPlan(origQuery).head

    if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
      val transform = origQuery.getHints.getTransformSchema
      SpatialRDD(sc.emptyRDD[SimpleFeature], transform.getOrElse(sft))
    } else {
      val query = ds.queryPlanner.configureQuery(sft, origQuery)
      val transform = query.getHints.getTransformSchema
      GeoMesaConfigurator.setSchema(conf, sft)
      GeoMesaConfigurator.setSerialization(conf)
      GeoMesaConfigurator.setIndexIn(conf, qp.filter.index)
      GeoMesaConfigurator.setTable(conf, qp.table.getNameAsString)
      transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

      // we need to pass the original filter all the way to the Spark workers so
      // that we enforce bbox'es and secondary filters.
      GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))

      val scans = qp.ranges.map {
        case scan: BigtableExtendedScan =>
          // need to set the table name in each scan
          scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.table.getName)
          BigtableInputFormatBase.scanToString(scan)

        case get: org.apache.hadoop.hbase.client.Get =>
          val bes = new BigtableExtendedScan()
          bes.addRange(get.getRow, ByteArrays.rowFollowingRow(get.getRow))
          bes.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.table.getName)
          BigtableInputFormatBase.scanToString(bes)

        case scan: org.apache.hadoop.hbase.client.Scan =>
          val bes = new BigtableExtendedScan()
          bes.addRange(scan.getStartRow, scan.getStopRow)
          bes.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.table.getName)
          BigtableInputFormatBase.scanToString(bes)
      }
      conf.setStrings(BigtableInputFormat.SCANS, scans: _*)

      val rdd = sc.newAPIHadoopRDD(conf, classOf[GeoMesaBigtableInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
      SpatialRDD(rdd, transform.getOrElse(sft))
    }
  }

}

class GeoMesaBigtableInputFormat extends InputFormat[Text, SimpleFeature] {
  var delegate: BigtableInputFormat = _

  var sft: SimpleFeatureType = _
  var table: HBaseIndexAdapter = _

  private def init(conf: Configuration): Unit = if (sft == null) {
    sft = GeoMesaConfigurator.getSchema(conf)
    table = HBaseFeatureIndex.index(GeoMesaConfigurator.getIndexIn(conf)).asInstanceOf[HBaseIndexAdapter]
    delegate = new BigtableInputFormat(TableName.valueOf(GeoMesaConfigurator.getTable(conf)))
    delegate.setConf(conf)
    // see TableMapReduceUtil.java
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
    conf.set(TableInputFormat.INPUT_TABLE, GeoMesaConfigurator.getTable(conf))
  }

  /**
    * Gets splits for a job.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val splits = delegate.getSplits(context)
    splits
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    init(context.getConfiguration)
    val rr = delegate.createRecordReader(split, context)
    val transform = GeoMesaConfigurator.getTransformSchema(context.getConfiguration)
    val ecql = GeoMesaConfigurator.getFilter(context.getConfiguration).map(FastFilterFactory.toFilter(sft, _))
    new HBaseGeoMesaRecordReader(table, sft, ecql, transform, rr, false)
  }
}

object BigtableInputFormat {
  /** Job parameter that specifies the scan list. */
  val SCANS = "hbase.mapreduce.scans"
}

class BigtableInputFormat(val name: TableName) extends BigtableInputFormatBase with Configurable {
  setName(name)

  /** The configuration. */
  private var conf: Configuration = _


  /**
    * Returns the current configuration.
    *
    * @return The current configuration.
    * @see org.apache.hadoop.conf.Configurable#getConf()
    */
  def getConf: Configuration = conf

  /**
    * Sets the configuration. This is used to set the details for the tables to
    * be scanned.
    *
    * @param configuration The configuration to set.
    * @see   org.apache.hadoop.conf.Configurable#setConf(
    *        org.apache.hadoop.conf.Configuration)
    */
  def setConf(configuration: Configuration): Unit = {
    this.conf = configuration
    val rawScans = conf.getStrings(BigtableInputFormat.SCANS)
    if (rawScans.length <= 0) throw new IllegalArgumentException("There must be at least 1 scan configuration set to : " + BigtableInputFormat.SCANS)
    val s = new java.util.ArrayList[Scan]
    rawScans.foreach { r => s.add(BigtableInputFormatBase.stringToScan(r)) }
    setScans(s)
  }
}
