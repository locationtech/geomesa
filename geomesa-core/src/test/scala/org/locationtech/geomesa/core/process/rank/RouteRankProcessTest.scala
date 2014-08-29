package org.locationtech.geomesa.core.process.rank

import org.geotools.data.DataStoreFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.core.index.{Constants, IndexSchemaBuilder}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class RouteRankProcessTest extends Specification {

  sequential

  val dtgField = org.locationtech.geomesa.core.process.tube.DEFAULT_DTG_FIELD
  val geotimeAttributes = s"*geom:Geometry,$dtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "indexSchemaFormat" -> new IndexSchemaBuilder("~").randomNumber(3).constant("TEST").geoHash(0, 3).date("yyyyMMdd").nextPart().geoHash(3, 2).nextPart().id().build(),
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

  val sftRouteName = "routeRankProcessInputType"
  val sftRoute = SimpleFeatureTypes.createType(sftRouteName, s"routeid:String,$geotimeAttributes")
  sftRoute.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  val ds = createStore

  ds.createSchema(sftRoute)
  val fsRoute = ds.getFeatureSource(sftRouteName).asInstanceOf[AccumuloFeatureStore]

  def buildSF(sft: SimpleFeatureType, id: String, ds: String = "2011-01-01T00:00:00Z") = {
    val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), id)
    sf.setAttribute(dtgField, new DateTime(ds, DateTimeZone.UTC).toDate)
    sf
  }

  val routeFeatureCollection = new DefaultFeatureCollection(sftRouteName, sftRoute)
  // the cville route from RouteTest
  val cvilleRouteCoords = Seq((-78.5, 38.0), (-78.4, 38.2), (-78.41, 38.25), (-78.49, 38.23))
  val routeLineString =
    cvilleRouteCoords
      .map { case (x, y) => s"$x $y"}
      .mkString("LINESTRING(", ",", ")")
  val routeSF = buildSF(sftRoute, "theRoute")
  routeSF.setAttribute("routeid", "1")
  routeSF.setDefaultGeometry(WKTUtils.read(routeLineString))
  routeFeatureCollection.add(routeSF)

  // write the feature to the store
  val routeRes = fsRoute.addFeatures(routeFeatureCollection)


  val dataSFTName = "routeRankProcessDataType"
  val dataSFT = SimpleFeatureTypes.createType(dataSFTName, s"key:String,$geotimeAttributes")
  dataSFT.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  ds.createSchema(dataSFT)
  val dataFeatureSource = ds.getFeatureSource(dataSFTName).asInstanceOf[AccumuloFeatureStore]

  val dataFeatureCollection = new DefaultFeatureCollection(dataSFTName, dataSFT)

  cvilleRouteCoords.zipWithIndex.foreach {
    case ((x, y), i) =>
      val minutes = 10 * i
      val sf = buildSF(dataSFT, "cville" + i, f"2011-01-01T00:$minutes%02d:00Z")
      sf.setAttribute("key", "orig")
      sf.setDefaultGeometry(WKTUtils.read(s"POINT($x $y)"))
      dataFeatureCollection.add(sf)
  }

  cvilleRouteCoords.zipWithIndex.foreach {
    case ((x, y), i) =>
      val sf = buildSF(dataSFT, "tfcville" + i) // all dates the same, instantaneous travel!
      sf.setAttribute("key", "toofast")
      sf.setDefaultGeometry(WKTUtils.read(s"POINT($x $y)"))
      dataFeatureCollection.add(sf)
  }

  cvilleRouteCoords.reverse.zipWithIndex.foreach {
    case ((x, y), i) =>
      val minutes = 10 * i
      val sf = buildSF(dataSFT, "revcville" + i, f"2011-01-01T00:$minutes%02d:00Z")
      sf.setAttribute("key", "reversed")
      sf.setDefaultGeometry(WKTUtils.read(s"POINT($x $y)"))
      dataFeatureCollection.add(sf)
  }

  val dataRes = dataFeatureSource.addFeatures(dataFeatureCollection)

  "RouteRankProcess" should {

    val epsilon = 0.0005
    def be_~(x: Double) = beCloseTo(x, epsilon)

    "appropriately rank features along a route" in {
      val routeFeature = fsRoute.getFeatures()
      val dataFeatures = dataFeatureSource.getFeatures(CQL.toFilter("key = 'orig'"))
      val process = new RouteRankProcess
      val results = process.execute(routeFeature, dataFeatures, 1000.0, "key", 0, -1, "combined.score")

      results.results.size must_== 1
      val result = results.results.head
      result.key must_== "orig"
      result.combined.score must be_~(0.046)
    }

    "gracefully handle tracks with no consistent motion" in {
      val routeFeature = fsRoute.getFeatures()
      val dataFeatures = dataFeatureSource.getFeatures(CQL.toFilter("key = 'toofast'"))
      val process = new RouteRankProcess
      val results = process.execute(routeFeature, dataFeatures, 1000.0, "key", 0, -1, "combined.score")

      results.results.size must_== 1
      val result = results.results.head
      result.key must_== "toofast"
      result.combined.score must be_~(0.0)
    }

    "rank reverse route equal to original route" in {
      val routeFeature = fsRoute.getFeatures()
      val dataFeatures = dataFeatureSource.getFeatures(CQL.toFilter("key = 'orig' or key = 'reversed'"))
      val process = new RouteRankProcess
      val results = process.execute(routeFeature, dataFeatures, 1000.0, "key", 0, -1, "combined.score")

      for (rvb <- results.results)
        rvb.combined.score must be_~(0.046)
      results.results.size must_== 2
    }

  }
}
