package geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import geomesa.core.data.SimpleFeatureEncoderFactory
import geomesa.core.index._
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import java.util
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.DataUtilities
import org.joda.time.{DateTime, DateTimeZone}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random

object TestData extends Logging {
  val TEST_USER = "root"
  val TEST_TABLE = "test_table"
  val TEST_AUTHORIZATIONS = Constants.NO_AUTHS
  val emptyBytes = new Value(Array[Byte]())

  case class Entry(wkt: String, id: String, dt: DateTime = new DateTime(defaultDateTime))

  // set up the geographic query polygon
  val wktQuery = "POLYGON((45 23, 48 23, 48 27, 45 27, 45 23))"

  val featureEncoder = SimpleFeatureEncoderFactory.defaultEncoder
  val featureName = "feature"
  val schemaEncoding = "%~#s%" + featureName + "#cstr%10#r%0,1#gh%yyyyMM#d::%~#s%1,3#gh::%~#s%4,3#gh%ddHH#d%10#id"

  def getFeatureType = {
    val ft: SimpleFeatureType = SimpleFeatureTypes.createType(featureName, UnitTestEntryType.getTypeSpec)
    ft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
    ft
  }

  // This is a quick trick to make sure that the userData is set.
  lazy val featureType: SimpleFeatureType = getFeatureType

  val index = IndexSchema(schemaEncoding, featureType, featureEncoder)

  val defaultDateTime = new DateTime(2011, 6, 1, 0, 0, 0, DateTimeZone.forID("UTC")).toDate

  // utility function that can encode multiple types of geometry
  def createObject(id: String, wkt: String, dt: DateTime = new DateTime(defaultDateTime)): List[(Key, Value)] = {
    val geomType: String = wkt.split( """\(""").head
    val geometry: Geometry = WKTUtils.read(wkt)
    val entry =
      AvroSimpleFeatureFactory.buildAvroFeature(
        featureType,
        List(null, null, null, id, geometry, dt.toDate, dt.toDate),
        s"|data|$id")

    //entry.setAttribute(geomType, id)
    entry.setAttribute("attr2", "2nd" + id)
    index.encode(entry).toList
  }

  def createSF(e: Entry): SimpleFeature = {
    val geometry: Geometry = WKTUtils.read(e.wkt)
    val entry =
      AvroSimpleFeatureFactory.buildAvroFeature(
        featureType,
        List(null, null, null, null, geometry, e.dt.toDate, e.dt.toDate),
        s"|data|${e.id}")
    entry.setAttribute("attr2", "2nd" + e.id)
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

  def encodeDataList(entries: List[Entry] = fullData): util.Collection[(Key, Value)] = {
    val list: List[(Key, Value)] =
      entries.flatMap { entry =>
        createObject(entry.id, entry.wkt, entry.dt)
      }.toList

    list.sortWith((kvA: (Key, Value), kvB: (Key, Value)) => kvA._1.toString < kvB._1.toString).asJavaCollection
  }

  def encodeDataMap(entries: List[Entry] = fullData): util.TreeMap[Key, Value] = {
    val list = encodeDataList(entries)

    val map = new util.TreeMap[Key, Value]()
    list.foreach(kv => map(kv._1) = kv._2)

    map
  }
}
