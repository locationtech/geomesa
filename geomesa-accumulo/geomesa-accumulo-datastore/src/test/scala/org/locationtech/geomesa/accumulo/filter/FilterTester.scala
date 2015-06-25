/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.filter

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.Query
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AllPredicateTest extends Specification with FilterTester {
  val filters = goodSpatialPredicates
  "all predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class AndGeomsPredicateTest extends FilterTester {
  val filters = andedSpatialPredicates
  "and geom predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class OrGeomsPredicateTest extends FilterTester {
  val filters = oredSpatialPredicates
  "or geom predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class OrGeomsPredicateWithProjectionTest extends FilterTester {
  val filters = oredSpatialPredicates
  "or geom predicates with projection" should {
    "filter correctly" in {
      runTest()
    }
  }

  override def modifyQuery(query: Query): Unit = query.setPropertyNames(Array("geom"))
}

@RunWith(classOf[JUnitRunner])
class BasicTemporalPredicateTest extends FilterTester {
  val filters = temporalPredicates
  "basic temporal predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class BasicSpatioTemporalPredicateTest extends FilterTester {
  val filters = spatioTemporalPredicates
  "basic spatiotemporal predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class AttributePredicateTest extends FilterTester {
  val filters = attributePredicates
  "attribute predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class AttributeGeoPredicateTest extends FilterTester {
  val filters = attributeAndGeometricPredicates
  "attribute geo predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

//@RunWith(classOf[JUnitRunner])
class DWithinPredicateTest extends FilterTester {   
  val filters = dwithinPointPredicates
  "dwithin predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class IdPredicateTest extends FilterTester {
  val filters = idPredicates
  "id predicates" should {
    "filter correctly" in {
      runTest()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class IdQueryTest extends Specification with TestWithDataStore {

  override val spec = "age:Int:index=true,name:String:index=true,dtg:Date,*geom:Point:srid=4326"

  val ff = CommonFactoryFinder.getFilterFactory2
  val geomBuilder = JTSFactoryFinder.getGeometryFactory
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val data = List(
    ("1", Array(10, "johndoe", new Date), geomBuilder.createPoint(new Coordinate(10, 10))),
    ("2", Array(20, "janedoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20))),
    ("3", Array(30, "johnrdoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20)))
  )
  val features = data.map { case (id, attrs, geom) =>
    builder.reset()
    builder.addAll(attrs.asInstanceOf[Array[AnyRef]])
    val f = builder.buildFeature(id)
    f.setDefaultGeometry(geom)
    f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    f
  }

  addFeatures(features)

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

trait FilterTester extends Specification with TestWithDataStore with Logging {

  override def spec = SimpleFeatureTypes.encodeType(TestData.featureType)

  val mediumDataFeatures: Seq[SimpleFeature] =
    TestData.mediumData.map(TestData.createSF).map(f => new ScalaSimpleFeature(f.getID, sft, f.getAttributes.toArray))

  addFeatures(mediumDataFeatures)

  def filters: Seq[String]

  def modifyQuery(query: Query): Unit = {}

  def compareFilter(filter: Filter) = {
    val filterCount = mediumDataFeatures.count(filter.evaluate)
    val query = new Query(sftName, filter)
    modifyQuery(query) // allow for tweaks in subclasses
    val queryCount = fs.getFeatures(query).size
    logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${mediumDataFeatures.size}: " +
      s"filter hits: $filterCount query hits: $queryCount")
    queryCount mustEqual filterCount
  }

  def runTest() = forall(filters.map(ECQL.toFilter))(compareFilter)
}
