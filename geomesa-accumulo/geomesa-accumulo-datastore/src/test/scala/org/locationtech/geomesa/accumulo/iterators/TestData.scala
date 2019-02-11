/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.DataStore
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureStore}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.accumulo.index.IndexValueEncoder
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureSerializers}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.util.Random

object TestData extends LazyLogging {
  val TEST_USER = "root"
  val TEST_TABLE = "test_table"
  val TEST_AUTHORIZATIONS = new Authorizations()
  val emptyBytes = new Value(Array[Byte]())

  case class Entry(wkt: String, id: String, dt: ZonedDateTime = defaultDateTime)

  // set up the geographic query polygon
  val wktQuery = "POLYGON((45 23, 48 23, 48 27, 45 27, 45 23))"

  val featureName = "feature"

  def getTypeSpec(suffix: String = "2") = {
    s"A_POINT:String,A_LINESTRING:String,A_POLYGON:String,attr$suffix:String:index=join," +
        s"geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date;geomesa.mixed.geometries=true"
  }

  def getFeatureType(typeNameSuffix: String = "", attrNameSuffix: String = "2") = {
    val fn = s"$featureName$typeNameSuffix"
    val ft: SimpleFeatureType = SimpleFeatureTypes.createType(fn, getTypeSpec(attrNameSuffix))
    ft.setDtgField("dtg")
    ft
  }

  def buildFeatureSource(ds: DataStore, featureType: SimpleFeatureType, features: Seq[SimpleFeature]): SimpleFeatureSource = {
    ds.createSchema(featureType)
    val fs = ds.getFeatureSource(featureType.getTypeName).asInstanceOf[SimpleFeatureStore]
    val coll = new DefaultFeatureCollection(featureType.getTypeName)
    coll.addAll(features.asJavaCollection)

    logger.debug(s"Adding SimpleFeatures of type ${coll.getSchema.getTypeName} to feature store.")
    fs.addFeatures(coll)
    logger.debug("Done adding SimpleFeatures to feature store.")

    fs
  }

  def getFeatureStore(ds: DataStore, simpleFeatureType: SimpleFeatureType, features: Seq[SimpleFeature]) = {
    val names = ds.getNames

    if(!names.contains(simpleFeatureType.getTypeName)) {
      buildFeatureSource(ds, simpleFeatureType, features)
    } else {
      ds.getFeatureSource(simpleFeatureType.getTypeName)
    }
  }

  // This is a quick trick to make sure that the userData is set.
  lazy val featureType: SimpleFeatureType = getFeatureType()

  lazy val featureEncoder = SimpleFeatureSerializers(getFeatureType(), SerializationType.AVRO)
  lazy val indexValueEncoder = IndexValueEncoder(featureType)

  val defaultDateTime = ZonedDateTime.of(2011, 6, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  def createSF(e: Entry): SimpleFeature = createSF(e, featureType)

  def createSF(e: Entry, sft: SimpleFeatureType): SimpleFeature = {
    val geometry: Geometry = WKTUtils.read(e.wkt)
    val entry =
      AvroSimpleFeatureFactory.buildAvroFeature(
        sft,
        List(null, null, null, null, geometry, Date.from(e.dt.toInstant), Date.from(e.dt.toInstant)),
        s"${e.id}")
    entry.setAttribute("attr2", "2nd" + e.id)
    entry.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    entry
  }

  val points = List[Entry](
    Entry("POINT(47.2 25.6)", "1"), // hit
    Entry("POINT(17.2 35.6)", "2"),
    Entry("POINT(87.2 15.6)", "3"),
    Entry("POINT(47.2 25.6)", "4"), // hit
    Entry("POINT(17.2 22.6)", "5"),
    Entry("POINT(-47.2 -25.6)", "6"),
    Entry("POINT(47.2 25.6)", "7"), // hit
    Entry("POINT(67.2 -25.6)", "8"),
    Entry("POINT(47.2 28.0)", "9"),
    Entry("POINT(47.2 25.6)", "10"), // hit
    Entry("POINT(47.2 25.6)", "11"), // hit
    Entry("POINT(47.2 25.6)", "12"), // hit
    Entry("POINT(47.2 25.6)", "13"), // hit
    Entry("POINT(50.2 30.6)", "14"),
    Entry("POINT(50.2 30.6)", "15"),
    Entry("POINT(50.2 30.6)", "16"),
    Entry("POINT(50.2 30.6)", "17"),
    Entry("POINT(50.2 30.6)", "18"),
    Entry("POINT(50.2 30.6)", "19"),
    Entry("POINT(47.2 25.6)", "20"), // hit
    Entry("POINT(47.2 25.6)", "21"), // hit
    Entry("POINT(47.2 25.6)", "22"), // hit
    Entry("POINT(47.2 25.6)", "23"), // hit
    Entry("POINT(47.2 25.6)", "24"), // hit
    Entry("POINT(47.2 25.6)", "25"), // hit
    Entry("POINT(47.2 25.6)", "26"), // hit
    Entry("POINT(47.2 25.6)", "27"), // hit
    Entry("POINT(47.2 25.6)", "111"), // hit
    Entry("POINT(47.2 25.6)", "112"), // hit
    Entry("POINT(47.2 25.6)", "113"), // hit
    Entry("POINT(50.2 30.6)", "114"),
    Entry("POINT(50.2 30.6)", "115"),
    Entry("POINT(50.2 30.6)", "116"),
    Entry("POINT(50.2 30.6)", "117"),
    Entry("POINT(50.2 30.6)", "118"),
    Entry("POINT(50.2 30.6)", "119")
  )

  // add some lines to this query, both qualifying and non-qualifying
  val lines = List(
    Entry("LINESTRING(47.28515625 25.576171875, 48 26, 49 27)", "201"),
    Entry("LINESTRING(-47.28515625 -25.576171875, -48 -26, -49 -27)", "202")
  )

  // add some polygons to this query, both qualifying and non-qualifying
  // NOTE:  Only the last of these will match the ColF set, because they tend
  //        to be decomposed into 15-bit (3-character) GeoHash cells.
  val polygons = List(
    Entry("POLYGON((44 24, 44 28, 49 27, 49 23, 44 24))", "301"),
    Entry("POLYGON((-44 -24, -44 -28, -49 -27, -49 -23, -44 -24))", "302"),
    Entry("POLYGON((47.28515625 25.576171875, 47.28515626 25.576171876, 47.28515627 25.576171875, 47.28515625 25.576171875))", "303")
  )

  val fullData = points ::: lines ::: polygons

  val hugeData: Seq[Entry] = generateTestData(50000)

  val mediumData: Seq[Entry] = generateTestData(1000)

  def generateTestData(num: Int) = {
    val rng = new Random(0)
    val minTime = ZonedDateTime.of(2010, 6, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant.toEpochMilli
    val maxTime = ZonedDateTime.of(2010, 8, 31, 23, 59, 59, 999000000, ZoneOffset.UTC).toInstant.toEpochMilli

    val pts = (1 to num).map(i => {
      val wkt = "POINT(" +
        (40.0 + 10.0 * rng.nextDouble()).toString + " " +
        (20.0 + 10.0 * rng.nextDouble()).toString + " " +
        ")"
      val dt = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(math.round(minTime + (maxTime - minTime) * rng.nextDouble())),
        ZoneOffset.UTC
      )
      Entry(wkt, (100000 + i).toString, dt)
    }).toList

    val gf = new GeometryFactory()

    val linesPolys = pts.grouped(3).take(num/50).flatMap { threeEntries =>
      val headEntry = threeEntries.head

      val threeCoords = threeEntries.map(e => WKTUtils.read(e.wkt).getCoordinate)

      val lineString = gf.createLineString(threeCoords.toArray)
      val poly = gf.createPolygon((threeCoords :+ threeCoords.head).toArray)

      val lsEntry = Entry(lineString.toString, headEntry.id+1000000, headEntry.dt)
      val polyEntry = Entry(poly.toString, headEntry.id+2000000, headEntry.dt)
      Seq(lsEntry, polyEntry)
    }


    pts ++ linesPolys
  }

  val pointWithNoID = List(Entry("POINT(-78.0 38.0)", null))

  val shortListOfPoints = List[Entry](
    Entry("POINT(47.2 25.6)", "1"), // hit
    Entry("POINT(47.2 25.6)", "7"), // hit
    Entry("POINT(50.2 30.6)", "117"),
    Entry("POINT(50.2 30.6)", "118"),
    Entry("POINT(47.2 25.6)", "4")
  )

  // this point's geohash overlaps with the query polygon so is a candidate result
  // however, the point itself is outside of the candidate result
  val geohashHitActualNotHit = List(Entry("POINT(47.999962 22.999969)", "9999"))

  // 49 features
  val includedDwithinPoints = Seq(
    "POINT (-69.860982683110791 25.670999804594551)",
    "POINT (-63.997858975742645 18.9943062994308)",
    "POINT (-81.830327678750578 37.085775640526542)",
    "POINT (-89.490770137868509 44.106303328073992)",
    "POINT (-64.863769432654507 20.078089354931279)",
    "POINT (-76.746987331939451 30.754452084293632)",
    "POINT (-83.143545929322613 38.85610727559537)",
    "POINT (-83.492602490558113 37.349478312620306)",
    "POINT (-69.679833785334282 24.388803458126716)",
    "POINT (-77.217346548139218 33.10117253660443)",
    "POINT (-48.624646482440973 4.055888157616433)",
    "POINT (-59.965969544921109 13.73922529393128)",
    "POINT (-69.719571567790766 25.42188199205567)",
    "POINT (-49.861755695550691 5.008207149016378)",
    "POINT (-59.53028948688214 13.666221546357587)",
    "POINT (-78.573811951638518 33.748446969499653)",
    "POINT (-75.148246144186032 29.088689349563502)",
    "POINT (-78.977185458964598 34.508762904115628)",
    "POINT (-45.145757200454497 -0.675717483534498)",
    "POINT (-70.814939235491693 24.670046948143156)",
    "POINT (-63.816714527267649 18.489239068296545)",
    "POINT (-54.20652730539409 9.01394728018499)",
    "POINT (-71.982651812779181 26.781538560326045)",
    "POINT (-51.71074903691521 7.783630450718865)",
    "POINT (-57.277254589777193 11.028044049316886)",
    "POINT (-51.778519694248303 7.700192534033889)",
    "POINT (-54.576171577496979 9.411552717211283)",
    "POINT (-58.018434745348337 13.069053319459581)",
    "POINT (-79.297793388564656 33.297052361806031)",
    "POINT (-54.041752176309622 8.401677730812796)",
    "POINT (-77.022561401567557 31.287987114079616)",
    "POINT (-54.273277144188896 8.423007576210081)",
    "POINT (-82.635439242627612 37.220921020638443)",
    "POINT (-66.240260183377984 22.333298874601866)",
    "POINT (-63.174079891818458 18.590732503333914)",
    "POINT (-49.604756624845336 3.603030252579086)",
    "POINT (-51.052335953192923 7.155692678339275)",
    "POINT (-79.426274623480495 34.457318692249387)",
    "POINT (-50.914821524842488 5.997763902901978)",
    "POINT (-58.002088202256417 13.707130381901802)",
    "POINT (-82.754970200843246 37.225536788891802)",
    "POINT (-58.739640682739136 13.619726121902358)",
    "POINT (-85.512639282423464 40.180830488630278)",
    "POINT (-88.352340439082099 44.029501612210311)",
    "POINT (-71.510589787816485 27.40689166758548)",
    "POINT (-47.028488437877314 2.675396523547844)",
    "POINT (-69.025674259692593 23.367911342771055)",
    "POINT (-67.336206873060874 22.855689772550061)",
    "POINT (-45.821184492445006 1.02615639387446)",
    "POINT (-59.943416863142957 15.425391686851068)"
  )

  // 150 features
  val excludedDwithinPoints = Seq(
    "POINT (-64.280357700776648 45.0)",
    "POINT (-60.606123086601883 45.0)",
    "POINT (-56.931888472427119 45.0)",
    "POINT (-53.257653858252354 45.0)",
    "POINT (-49.583419244077589 45.0)",
    "POINT (-45.909184629902825 45.0)",
    "POINT (-90.0 41.325765385825235)",
    "POINT (-82.651530771650471 41.325765385825235)",
    "POINT (-78.977296157475706 41.325765385825235)",
    "POINT (-75.303061543300942 41.325765385825235)",
    "POINT (-71.628826929126177 41.325765385825235)",
    "POINT (-67.954592314951412 41.325765385825235)",
    "POINT (-64.280357700776648 41.325765385825235)",
    "POINT (-60.606123086601883 41.325765385825235)",
    "POINT (-56.931888472427119 41.325765385825235)",
    "POINT (-53.257653858252354 41.325765385825235)",
    "POINT (-49.583419244077589 41.325765385825235)",
    "POINT (-45.909184629902825 41.325765385825235)",
    "POINT (-90.0 37.651530771650471)",
    "POINT (-86.325765385825235 37.651530771650471)",
    "POINT (-78.977296157475706 37.651530771650471)",
    "POINT (-75.303061543300942 37.651530771650471)",
    "POINT (-71.628826929126177 37.651530771650471)",
    "POINT (-67.954592314951412 37.651530771650471)",
    "POINT (-64.280357700776648 37.651530771650471)",
    "POINT (-60.606123086601883 37.651530771650471)",
    "POINT (-56.931888472427119 37.651530771650471)",
    "POINT (-53.257653858252354 37.651530771650471)",
    "POINT (-49.583419244077589 37.651530771650471)",
    "POINT (-45.909184629902825 37.651530771650471)",
    "POINT (-90.0 33.977296157475706)",
    "POINT (-86.325765385825235 33.977296157475706)",
    "POINT (-82.651530771650471 33.977296157475706)",
    "POINT (-75.303061543300942 33.977296157475706)",
    "POINT (-71.628826929126177 33.977296157475706)",
    "POINT (-67.954592314951412 33.977296157475706)",
    "POINT (-64.280357700776648 33.977296157475706)",
    "POINT (-60.606123086601883 33.977296157475706)",
    "POINT (-56.931888472427119 33.977296157475706)",
    "POINT (-53.257653858252354 33.977296157475706)",
    "POINT (-49.583419244077589 33.977296157475706)",
    "POINT (-45.909184629902825 33.977296157475706)",
    "POINT (-90.0 30.303061543300938)",
    "POINT (-86.325765385825235 30.303061543300938)",
    "POINT (-82.651530771650471 30.303061543300938)",
    "POINT (-78.977296157475706 30.303061543300938)",
    "POINT (-71.628826929126177 30.303061543300938)",
    "POINT (-67.954592314951412 30.303061543300938)",
    "POINT (-64.280357700776648 30.303061543300938)",
    "POINT (-60.606123086601883 30.303061543300938)",
    "POINT (-56.931888472427119 30.303061543300938)",
    "POINT (-53.257653858252354 30.303061543300938)",
    "POINT (-49.583419244077589 30.303061543300938)",
    "POINT (-45.909184629902825 30.303061543300938)",
    "POINT (-90.0 26.62882692912617)",
    "POINT (-86.325765385825235 26.62882692912617)",
    "POINT (-82.651530771650471 26.62882692912617)",
    "POINT (-78.977296157475706 26.62882692912617)",
    "POINT (-75.303061543300942 26.62882692912617)",
    "POINT (-67.954592314951412 26.62882692912617)",
    "POINT (-64.280357700776648 26.62882692912617)",
    "POINT (-60.606123086601883 26.62882692912617)",
    "POINT (-56.931888472427119 26.62882692912617)",
    "POINT (-53.257653858252354 26.62882692912617)",
    "POINT (-49.583419244077589 26.62882692912617)",
    "POINT (-45.909184629902825 26.62882692912617)",
    "POINT (-90.0 22.954592314951402)",
    "POINT (-86.325765385825235 22.954592314951402)",
    "POINT (-82.651530771650471 22.954592314951402)",
    "POINT (-78.977296157475706 22.954592314951402)",
    "POINT (-75.303061543300942 22.954592314951402)",
    "POINT (-71.628826929126177 22.954592314951402)",
    "POINT (-64.280357700776648 22.954592314951402)",
    "POINT (-60.606123086601883 22.954592314951402)",
    "POINT (-56.931888472427119 22.954592314951402)",
    "POINT (-53.257653858252354 22.954592314951402)",
    "POINT (-49.583419244077589 22.954592314951402)",
    "POINT (-45.909184629902825 22.954592314951402)",
    "POINT (-90.0 19.280357700776634)",
    "POINT (-86.325765385825235 19.280357700776634)",
    "POINT (-82.651530771650471 19.280357700776634)",
    "POINT (-78.977296157475706 19.280357700776634)",
    "POINT (-75.303061543300942 19.280357700776634)",
    "POINT (-71.628826929126177 19.280357700776634)",
    "POINT (-67.954592314951412 19.280357700776634)",
    "POINT (-60.606123086601883 19.280357700776634)",
    "POINT (-56.931888472427119 19.280357700776634)",
    "POINT (-53.257653858252354 19.280357700776634)",
    "POINT (-49.583419244077589 19.280357700776634)",
    "POINT (-45.909184629902825 19.280357700776634)",
    "POINT (-90.0 15.606123086601865)",
    "POINT (-86.325765385825235 15.606123086601865)",
    "POINT (-82.651530771650471 15.606123086601865)",
    "POINT (-78.977296157475706 15.606123086601865)",
    "POINT (-75.303061543300942 15.606123086601865)",
    "POINT (-71.628826929126177 15.606123086601865)",
    "POINT (-67.954592314951412 15.606123086601865)",
    "POINT (-64.280357700776648 15.606123086601865)",
    "POINT (-56.931888472427119 15.606123086601865)",
    "POINT (-53.257653858252354 15.606123086601865)",
    "POINT (-49.583419244077589 15.606123086601865)",
    "POINT (-45.909184629902825 15.606123086601865)",
    "POINT (-90.0 11.931888472427097)",
    "POINT (-86.325765385825235 11.931888472427097)",
    "POINT (-82.651530771650471 11.931888472427097)",
    "POINT (-78.977296157475706 11.931888472427097)",
    "POINT (-75.303061543300942 11.931888472427097)",
    "POINT (-71.628826929126177 11.931888472427097)",
    "POINT (-67.954592314951412 11.931888472427097)",
    "POINT (-64.280357700776648 11.931888472427097)",
    "POINT (-60.606123086601883 11.931888472427097)",
    "POINT (-53.257653858252354 11.931888472427097)",
    "POINT (-49.583419244077589 11.931888472427097)",
    "POINT (-45.909184629902825 11.931888472427097)",
    "POINT (-90.0 8.257653858252329)",
    "POINT (-86.325765385825235 8.257653858252329)",
    "POINT (-82.651530771650471 8.257653858252329)",
    "POINT (-78.977296157475706 8.257653858252329)",
    "POINT (-75.303061543300942 8.257653858252329)",
    "POINT (-71.628826929126177 8.257653858252329)",
    "POINT (-67.954592314951412 8.257653858252329)",
    "POINT (-64.280357700776648 8.257653858252329)",
    "POINT (-60.606123086601883 8.257653858252329)",
    "POINT (-56.931888472427119 8.257653858252329)",
    "POINT (-49.583419244077589 8.257653858252329)",
    "POINT (-45.909184629902825 8.257653858252329)",
    "POINT (-90.0 4.583419244077562)",
    "POINT (-86.325765385825235 4.583419244077562)",
    "POINT (-82.651530771650471 4.583419244077562)",
    "POINT (-78.977296157475706 4.583419244077562)",
    "POINT (-75.303061543300942 4.583419244077562)",
    "POINT (-71.628826929126177 4.583419244077562)",
    "POINT (-67.954592314951412 4.583419244077562)",
    "POINT (-64.280357700776648 4.583419244077562)",
    "POINT (-60.606123086601883 4.583419244077562)",
    "POINT (-56.931888472427119 4.583419244077562)",
    "POINT (-53.257653858252354 4.583419244077562)",
    "POINT (-45.909184629902825 4.583419244077562)",
    "POINT (-90.0 0.909184629902795)",
    "POINT (-86.325765385825235 0.909184629902795)",
    "POINT (-82.651530771650471 0.909184629902795)",
    "POINT (-78.977296157475706 0.909184629902795)",
    "POINT (-75.303061543300942 0.909184629902795)",
    "POINT (-71.628826929126177 0.909184629902795)",
    "POINT (-67.954592314951412 0.909184629902795)",
    "POINT (-64.280357700776648 0.909184629902795)",
    "POINT (-60.606123086601883 0.909184629902795)",
    "POINT (-56.931888472427119 0.909184629902795)",
    "POINT (-53.257653858252354 0.909184629902795)",
    "POINT (-49.583419244077589 0.909184629902795)"
  )

}
