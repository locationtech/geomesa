package org.locationtech.geomesa.core.data

import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.filter.Filter
import org.opengis.filter.sort.{SortBy, SortOrder}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

trait GeoToolsFeatureStoreTester extends Specification with AccumuloDataStoreDefaults {
  def sftName: String

  "GeoTools Feature Stores" should {
    "handle sorting by attribute including nulls" in {
      val sftSpec = "string:String,int:Integer,dtg:Date,geom:Point:srid=4326,float:Float"
      val sft = createSchema(sftName, sftSpec)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      val sfBuilder = new SimpleFeatureBuilder(sft)

      "support sorting" >> {
        fs.getQueryCapabilities.supportsSorting(Array(SortBy.NATURAL_ORDER)) must beTrue
        fs.getQueryCapabilities.supportsSorting(Array(ff.sort("dtg", SortOrder.ASCENDING))) must beTrue
        fs.getQueryCapabilities.supportsSorting(Array(ff.sort("dtg", SortOrder.DESCENDING))) must beTrue
      }

      val l = List(
        ("joe", 23, new DateTime("2014-01-04").toDate, gf.createPoint(new Coordinate(0, 0)), 1.234),
        ("adam", 87, new DateTime("2014-01-05").toDate, gf.createPoint(new Coordinate(1, 0)), -2.345),
        ("jake", 45, new DateTime("2014-01-02").toDate, gf.createPoint(new Coordinate(-1, 0)), 3.452),
        (null, 45, new DateTime("2014-01-01").toDate, gf.createPoint(new Coordinate(5, 0)), 11.123),
        ("anna", null, new DateTime("2014-01-10").toDate, gf.createPoint(new Coordinate(-5, 0)), -1.427),
        ("drake", 35, null, gf.createPoint(new Coordinate(150, 0)), -4.212),
        ("carter", 185, new DateTime("2014-02-01").toDate, gf.createPoint(new Coordinate(-150, 0)), null),
        ("shelby", 2, new DateTime("2014-01-03").toDate, gf.createPoint(new Coordinate(0, 1)), 5.753),
        ("sarah", -5, new DateTime("2014-01-07").toDate, gf.createPoint(new Coordinate(-1, 1)), 21.642),
        ("claire", 7, new DateTime("2014-01-06").toDate, gf.createPoint(new Coordinate(0, -1)), 14.321)
      )

      val featList = l.zipWithIndex.map { case (features, index) =>
        features.productIterator.foreach {
          sfBuilder.add
        }
        sfBuilder.buildFeature(s"$index")
      }

      fs.addFeatures(DataUtilities.collection(featList))

      "sort ascending on String" >> {
        val dtgAscendingQ = new Query("test", Filter.INCLUDE)
        dtgAscendingQ.setSortBy(Array(ff.sort("string", SortOrder.ASCENDING)))

        val features = fs.getFeatures(dtgAscendingQ).features().toIterator
        val res: Seq[Option[String]] = features.map(feature => Option(feature.getAttribute("string")) match {
          case Some(string) => Option(string.asInstanceOf[String])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res must beSorted
        res.head must beLessThan(res(1))
      }

      "sort descending on String" >> {
        val dtgDescending = new Query("test", Filter.INCLUDE)
        dtgDescending.setSortBy(Array(ff.sort("string", SortOrder.DESCENDING)))

        val features = fs.getFeatures(dtgDescending).features().toIterator
        val res: Seq[Option[String]] = features.map(feature => Option(feature.getAttribute("string")) match {
          case Some(string) => Option(string.asInstanceOf[String])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res.reverse must beSorted
        res.head must beGreaterThan(res(1))
      }

      "sort ascending on Integer" >> {
        val dtgAscendingQ = new Query("test", Filter.INCLUDE)
        dtgAscendingQ.setSortBy(Array(ff.sort("int", SortOrder.ASCENDING)))

        val features = fs.getFeatures(dtgAscendingQ).features().toIterator
        val res: Seq[Option[Integer]] = features.map(feature => Option(feature.getAttribute("int")) match {
          case Some(int) => Option(int.asInstanceOf[Integer])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res must beSorted
        res.head must beLessThan(res(1))
      }

      "sort descending on Integer" >> {
        val dtgDescending = new Query("test", Filter.INCLUDE)
        dtgDescending.setSortBy(Array(ff.sort("int", SortOrder.DESCENDING)))

        val features = fs.getFeatures(dtgDescending).features().toIterator
        val res: Seq[Option[Integer]] = features.map(feature => Option(feature.getAttribute("int")) match {
          case Some(int) => Option(int.asInstanceOf[Integer])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res.reverse must beSorted
        res.head must beGreaterThan(res(1))
      }

      "sort ascending on Date" >> {
        val dtgAscendingQ = new Query("test", Filter.INCLUDE)
        dtgAscendingQ.setSortBy(Array(ff.sort("dtg", SortOrder.ASCENDING)))

        val features = fs.getFeatures(dtgAscendingQ).features().toIterator
        val res: Seq[Option[Long]] = features.map(feature => Option(feature.getAttribute("dtg")) match {
          case Some(date) => Option(date.asInstanceOf[Date].getTime)
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res must beSorted
        res.head must beLessThan(res(1))
      }

      "sort descending on Date" >> {
        val dtgDescending = new Query("test", Filter.INCLUDE)
        dtgDescending.setSortBy(Array(ff.sort("dtg", SortOrder.DESCENDING)))

        val features = fs.getFeatures(dtgDescending).features().toIterator
        val res: Seq[Option[Long]] = features.map(feature => Option(feature.getAttribute("dtg")) match {
          case Some(date) => Option(date.asInstanceOf[Date].getTime)
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res.reverse must beSorted
        res.head must beGreaterThan(res(1))
      }

      "sort ascending on Float" >> {
        val dtgAscendingQ = new Query("test", Filter.INCLUDE)
        dtgAscendingQ.setSortBy(Array(ff.sort("float", SortOrder.ASCENDING)))

        val features = fs.getFeatures(dtgAscendingQ).features().toIterator
        val res: Seq[Option[Float]] = features.map(feature => Option(feature.getAttribute("float")) match {
          case Some(float) => Option(float.asInstanceOf[Float])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res must beSorted
        res.head must beLessThan(res(1))
      }

      "sort descending on Float" >> {
        val dtgDescending = new Query("test", Filter.INCLUDE)
        dtgDescending.setSortBy(Array(ff.sort("float", SortOrder.DESCENDING)))

        val features = fs.getFeatures(dtgDescending).features().toIterator
        val res: Seq[Option[Float]] = features.map(feature => Option(feature.getAttribute("float")) match {
          case Some(float) => Option(float.asInstanceOf[Float])
          case None => None
        }).toList

        res.size must beEqualTo(featList.size)
        res.reverse must beSorted
        res.head must beGreaterThan(res(1))
      }

      // This test case to show how the EXACT_COUNT Query Hint can be used.
      // Since we can't get table sizes for MockAccumulo, we can't test both directions.
      "also accept counting hints" >> {
        val exactCountQuery = new Query("test", Filter.INCLUDE)
        val sizeByQuery = fs.getFeatures(exactCountQuery).features.toList.size

        exactCountQuery.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)

        fs.getCount(exactCountQuery) must equalTo(sizeByQuery)
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureStoreTest extends GeoToolsFeatureStoreTester {
  "AccumuloFeatureStore" should {
    "handle time bounds" in {
      val sftName = "TimeBoundsTest"
      val sft = createSchema(sftName)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      val sfBuilder = new SimpleFeatureBuilder(sft)

      val defaultInterval = ds.getTimeBounds(sft.getTypeName)
      defaultInterval.getStartMillis must be equalTo 0

      val date1 = new DateTime("2014-01-02").toDate
      sfBuilder.addAll(List("johndoe", gf.createPoint(new Coordinate(0, 0)), date1))
      val f1 = sfBuilder.buildFeature("f1")

      fs.addFeatures(DataUtilities.collection(List(f1)))

      val secondInterval = ds.getTimeBounds(sft.getTypeName)
      secondInterval.getStartMillis must be equalTo date1.getTime
      secondInterval.getEndMillis must be equalTo date1.getTime

      val date2 = new DateTime("2014-01-03").toDate
      sfBuilder.addAll(List("johndoe", gf.createPoint(new Coordinate(0, 0)), date2))
      val f2 = sfBuilder.buildFeature("f2")

      val date3 = new DateTime("2014-01-01").toDate
      sfBuilder.addAll(List("johndoe", gf.createPoint(new Coordinate(0, 0)), date3))
      val f3 = sfBuilder.buildFeature("f3")

      fs.addFeatures(DataUtilities.collection(List(f2, f3)))

      val thirdInterval = ds.getTimeBounds(sft.getTypeName)
      thirdInterval.getStartMillis must be equalTo date3.getTime
      thirdInterval.getEndMillis must be equalTo date2.getTime
    }
  }

  override def sftName: String = "AccumuloFeatureStoreTest"
}

@RunWith(classOf[JUnitRunner])
class CachingAccumuloFeatureCollectionTest extends GeoToolsFeatureStoreTester {
  override val ds = DataStoreFinder.getDataStore(Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "tableName"         -> defaultTable,
    "useMock"           -> "true",
    "caching"           -> "true",
    "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

  override def sftName: String = "CachingAccumuloFeatureCollectionTest"
}
