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

package org.locationtech.geomesa.accumulo.filter

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureStore}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AllPredicateTest extends Specification with FilterTester {
  val filters = goodSpatialPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class AndGeomsPredicateTest extends FilterTester {
  val filters = andedSpatialPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class OrGeomsPredicateTest extends FilterTester {
  val filters = oredSpatialPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class OrGeomsPredicateWithProjectionTest extends FilterTester {
  val filters = oredSpatialPredicates
  runTest

  override def modifyQuery(query: Query): Unit = query.setPropertyNames(Array("geom"))
}

@RunWith(classOf[JUnitRunner])
class BasicTemporalPredicateTest extends FilterTester {
  val filters = temporalPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class BasicSpatioTemporalPredicateTest extends FilterTester {
  val filters = spatioTemporalPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class AttributePredicateTest extends FilterTester {
  val filters = attributePredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class AttributeGeoPredicateTest extends FilterTester {
  val filters = attributeAndGeometricPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class DWithinPredicateTest extends FilterTester {   
  val filters = dwithinPointPredicates
}

@RunWith(classOf[JUnitRunner])
class IdPredicateTest extends FilterTester {
  val filters = idPredicates
  runTest
}

@RunWith(classOf[JUnitRunner])
class IdQueryTest extends Specification {

  val ff = CommonFactoryFinder.getFilterFactory2
  val ds = {
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "idquerytest",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
  }
  val geomBuilder = JTSFactoryFinder.getGeometryFactory
  val sft = SimpleFeatureTypes.createType("idquerysft", "age:Int:index=true,name:String:index=true,dtg:Date,*geom:Point:srid=4326")
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME,"dtg")
  ds.createSchema(sft)
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val data = List(
    ("1", Array(10, "johndoe", new Date), geomBuilder.createPoint(new Coordinate(10, 10))),
    ("2", Array(20, "janedoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20))),
    ("3", Array(30, "johnrdoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20)))
  )
  val featureCollection = new DefaultFeatureCollection()
  val features = data.foreach { case (id, attrs, geom) =>
    builder.reset()
    builder.addAll(attrs.asInstanceOf[Array[AnyRef]])
    val f = builder.buildFeature(id)
    f.setDefaultGeometry(geom)
    f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    featureCollection.add(f)
  }
  val fs = ds.getFeatureSource("idquerysft").asInstanceOf[SimpleFeatureStore]
  fs.addFeatures(featureCollection)
  fs.flush()

  "Id queries" should {
    "use record table to return a result" >> {
      val idQ = ff.id(ff.featureId("2"))
      val res = fs.getFeatures(idQ).features().toList
      res.length mustEqual 1
      res.head.getID mustEqual "2"
    }

    "handle multiple ids correctly" >> {
      val idQ = ff.id(ff.featureId("1"), ff.featureId("3"))
      val res = fs.getFeatures(idQ).features().toList
      res.length mustEqual 2
      res.map(_.getID) must contain ("1", "3")
    }

    "return no events when multiple IDs ANDed result in no intersection"  >> {
      val idQ1 = ff.id(ff.featureId("1"), ff.featureId("3"))
      val idQ2 = ff.id(ff.featureId("2"))
      val idQ =  ff.and(idQ1, idQ2)
      val qRes = fs.getFeatures(idQ)
      val res= qRes.features().toList

      res.length mustEqual 0
    }
  }
}

object FilterTester extends Logging {
  val mediumDataFeatures: Seq[SimpleFeature] = TestData.mediumData.map(TestData.createSF)
  val sft = mediumDataFeatures.head.getFeatureType

  val ds = {
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "filtertester",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  def buildFeatureSource(): SimpleFeatureSource = {
    ds.createSchema(sft)
    val fs: AccumuloFeatureStore = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]
    val coll = new DefaultFeatureCollection(sft.getTypeName)
    coll.addAll(mediumDataFeatures)

    logger.debug("Adding SimpleFeatures to feature store.")
    fs.addFeatures(coll)
    logger.debug("Done adding SimpleFeaturest to feature store.")

    fs
  }
  val fs1 = buildFeatureSource()
  val afr = ds.getFeatureReader(sft.getTypeName)
}

trait FilterTester extends Specification with Logging {
  import FilterTester._
  import filter._

  val fs = fs1

  def filters: Seq[String]

  def modifyQuery(query: Query): Unit = {}

  def compareFilter(filter: Filter) = {
    logger.debug(s"Filter: ${ECQL.toCQL(filter)}")
    s"The filter $filter" should {
      "return the same number of results from filtering and querying" in {
        val filterCount = mediumDataFeatures.count(filter.evaluate)
        val queryCount = fs.getFeatures(filter).size
        logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${mediumDataFeatures.size}: " +
            s"filter hits: $filterCount query hits: $queryCount")
        queryCount mustEqual filterCount
      }
    }
  }
  def runTest = filters.map { s => compareFilter(s) }
}
