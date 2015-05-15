/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.data.{Mutation, Value}
import org.geotools.data.DataStore
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToWrite
import org.locationtech.geomesa.accumulo.data.{AccumuloFeatureStore, INTERNAL_GEOMESA_VERSION}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.features.{SimpleFeatureSerializers, SerializationType, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.util.Random

object UnitTestEntryType  {
  def getTypeSpec = "POINT:String," + "LINESTRING:String," + "POLYGON:String," + "attr2:String," + spec
}

object TestData extends Logging {
  val TEST_USER = "root"
  val TEST_TABLE = "test_table"
  val TEST_AUTHORIZATIONS = Constants.NO_AUTHS
  val emptyBytes = new Value(Array[Byte]())

  case class Entry(wkt: String, id: String, dt: DateTime = new DateTime(defaultDateTime))

  // set up the geographic query polygon
  val wktQuery = "POLYGON((45 23, 48 23, 48 27, 45 27, 45 23))"

  val featureName = "feature"
  val schemaEncoding =
    new IndexSchemaBuilder("~")
      .randomNumber(10)
      .indexOrDataFlag()
      .constant(featureName)
      .geoHash(0, 3)
      .date("yyyyMMdd")
      .nextPart()
      .geoHash(3, 2)
      .nextPart()
      .id()
      .build()

  def getTypeSpec(suffix: String = "2") = {
    s"POINT:String,LINESTRING:String,POLYGON:String,attr$suffix:String:index=true," + spec
  }

  def getFeatureType(typeNameSuffix: String = "", attrNameSuffix: String = "2", tableSharing: Boolean = true) = {
    val fn = s"$featureName$typeNameSuffix"
    val ft: SimpleFeatureType = SimpleFeatureTypes.createType(fn, getTypeSpec(attrNameSuffix))
    ft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

    setTableSharing(ft, tableSharing)
    ft
  }

  def buildFeatureSource(ds: DataStore, featureType: SimpleFeatureType, features: Seq[SimpleFeature]): SimpleFeatureSource = {
    ds.createSchema(featureType)
    val fs: AccumuloFeatureStore = ds.getFeatureSource(featureType.getTypeName).asInstanceOf[AccumuloFeatureStore]
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
  lazy val indexValueEncoder = IndexValueEncoder(featureType, INTERNAL_GEOMESA_VERSION)

  lazy val indexEncoder = IndexSchema.buildKeyEncoder(featureType, schemaEncoding)

  val defaultDateTime = new DateTime(2011, 6, 1, 0, 0, 0, DateTimeZone.forID("UTC")).toDate

  // utility function that can encode multiple types of geometry
  def createObject(id: String, wkt: String, dt: DateTime = new DateTime(defaultDateTime)): Seq[Mutation] = {
    val geomType: String = wkt.split( """\(""").head
    val geometry: Geometry = WKTUtils.read(wkt)
    val entry =
      AvroSimpleFeatureFactory.buildAvroFeature(
        featureType,
        List(null, null, null, id, geometry, dt.toDate, dt.toDate),
        s"|data|$id")

    //entry.setAttribute(geomType, id)
    entry.setAttribute("attr2", "2nd" + id)
    indexEncoder.synchronized {
      val toWrite = new FeatureToWrite(entry, "", featureEncoder, indexValueEncoder)
      indexEncoder.encode(toWrite)
    }
  }

  def createSF(e: Entry): SimpleFeature = createSF(e, featureType)

  def createSF(e: Entry, sft: SimpleFeatureType): SimpleFeature = {
    val geometry: Geometry = WKTUtils.read(e.wkt)
    val entry =
      AvroSimpleFeatureFactory.buildAvroFeature(
        sft,
        List(null, null, null, null, geometry, e.dt.toDate, e.dt.toDate),
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

  val allThePoints = (-180 to 180).map(lon => {
    val x = lon.toString
    val y = (lon / 2).toString
    Entry(s"POINT($x $y)", x)
  })

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
    val minTime = new DateTime(2010, 6, 1, 0, 0, 0, DateTimeZone.forID("UTC")).getMillis
    val maxTime = new DateTime(2010, 8, 31, 23, 59, 59, DateTimeZone.forID("UTC")).getMillis

    val pts = (1 to num).map(i => {
      val wkt = "POINT(" +
        (40.0 + 10.0 * rng.nextDouble()).toString + " " +
        (20.0 + 10.0 * rng.nextDouble()).toString + " " +
        ")"
      val dt = new DateTime(
        math.round(minTime + (maxTime - minTime) * rng.nextDouble()),
        DateTimeZone.forID("UTC")
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
}
