/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.data

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.geotools.data.{DataStore, DataStoreFinder}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
 * Provides values and functions used for testing
 * in AccumuloDataStoreTest and AccumuloFeatureStoreTest.
 */
trait AccumuloDataStoreDefaults {
  val ff = CommonFactoryFinder.getFilterFactory2
  val geotimeAttributes = org.locationtech.geomesa.accumulo.index.spec
  val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)
  val WGS84 = DefaultGeographicCRS.WGS84
  val gf = JTSFactoryFinder.getGeometryFactory

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  df.setTimeZone(TimeZone.getTimeZone("UTC"))

  val defaultSchema = "name:String,geom:Point:srid=4326,dtg:Date"
  val defaultGeom = WKTUtils.read("POINT(45.0 49.0)")
  val defaultDtg = new Date(100000)
  val defaultName = "testType"
  val defaultFid = "fid-1"

  val defaultTable = getClass.getSimpleName

  val dsParams = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "tableName"         -> defaultTable,
    "useMock"           -> "true",
    "featureEncoding"   -> "avro")
  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

  /**
   * Create a schema. Schema name should be unique, otherwise tests will interfere with each other.
   *
   * @param sftName
   * @param spec
   * @param dateField
   */
  def createSchema(sftName: String,
                   spec: String = defaultSchema,
                   dateField: Option[String] = Some("dtg"),
                   dataStore: DataStore = ds) = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    dateField.foreach(sft.setDtgField)
    dataStore.createSchema(sft)
    sft
  }

  /**
   * Adds a default feature to a feature store.
   *
   * @param sft
   * @param attributes
   * @param fid
   * @param dataStore
   */
  def addDefaultPoint(sft: SimpleFeatureType,
                      attributes: List[AnyRef] = List(defaultName, defaultGeom, defaultDtg),
                      fid: String = defaultFid,
                      dataStore: DataStore = ds) = {
    val fs = dataStore.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]

    // create a feature
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.addAll(attributes)
    val liveFeature = builder.buildFeature(fid)

    // make sure we ask the system to re-use the provided feature-ID
    liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
    featureCollection.add(liveFeature)
    fs.addFeatures(featureCollection)
  }

  /**
   * Creates 6 features.
   *
   * @param sft
   */
  def createTestFeatures(sft: SimpleFeatureType) = (0 until 6).map { i =>
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }
}
