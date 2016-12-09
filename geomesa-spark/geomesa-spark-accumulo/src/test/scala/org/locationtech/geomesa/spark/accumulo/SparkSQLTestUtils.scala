/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.accumulo

import java.io.File
import java.nio.file.Files
import java.util.{Map => JMap}

import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.minicluster.{MiniAccumuloConfig, MiniAccumuloCluster}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataUtilities, DataStoreFinder}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.tools.ingest.ConverterIngest
import org.locationtech.geomesa.utils.geotools.{GeneralShapefileIngest, SimpleFeatureTypes}

import scala.collection.JavaConversions._

object SparkSQLTestUtils {
  def setupMiniAccumulo(): MiniAccumuloCluster = {
    val randomDir = Files.createTempDirectory("mac").toFile
    val config = new MiniAccumuloConfig(randomDir, "password").setJDWPEnabled(true)
    val mac = new MiniAccumuloCluster(config)
    mac.start()
    randomDir.deleteOnExit()

    mac
  }

  def createDataStoreParams(mac: MiniAccumuloCluster): JMap[String, String] = {
    val dsParams: JMap[String, String] = Map(
      // "connector" -> connector,
      AccumuloDataStoreParams.zookeepersParam.getName -> mac.getZooKeepers,
      AccumuloDataStoreParams.instanceIdParam.getName -> mac.getInstanceName,
      AccumuloDataStoreParams.userParam.getName -> "root",
      AccumuloDataStoreParams.passwordParam.getName -> "password",
      "caching" -> "false",
      // note the table needs to be different to prevent testing errors
      AccumuloDataStoreParams.tableNameParam.getName -> "sparksql")
    dsParams
  }

  def ingestChicago(ds: AccumuloDataStore): Unit = {
    // Chicago data ingest
    val sft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")
    ds.createSchema(sft)

    val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

    val parseDate = ISODateTimeFormat.basicDateTime().parseDateTime _
    val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

    val features = DataUtilities.collection(List(
      new ScalaSimpleFeature("1", sft, initialValues = Array("true","1",parseDate("20160101T000000.000Z").toDate, createPoint(new Coordinate(-76.5, 38.5)))),
      new ScalaSimpleFeature("2", sft, initialValues = Array("true","2",parseDate("20160102T000000.000Z").toDate, createPoint(new Coordinate(-77.0, 38.0)))),
      new ScalaSimpleFeature("3", sft, initialValues = Array("true","3",parseDate("20160103T000000.000Z").toDate, createPoint(new Coordinate(-78.0, 39.0))))
    ))

    fs.addFeatures(features)
  }

  def ingestGeoNames(dsParams: JMap[String, String]): Unit = {
    // GeoNames ingest
    val file = new File(getClass.getResource("/geonames-sample.tsv").getFile)
    val filePath = file.getAbsolutePath
    val ingest = new ConverterIngest(GeoNames.sft, dsParams.toMap, GeoNames.conf, Seq(filePath), "", Iterator.empty, 16)
    ingest.run
  }

  def ingestStates(ds: AccumuloDataStore) = {
    // States shapefile ingest
    val file = new File(getClass.getResource("/states.shp").getFile)
    val filePath = file.getAbsolutePath
    GeneralShapefileIngest.shpToDataStore(filePath, ds, "states")
  }
}

