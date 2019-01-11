/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query, _}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class KuduDataStoreIntegrationTest extends Specification {

  import scala.collection.JavaConverters._

  skipAll // integration test

  sequential

  val params = Map(
    "kudu.master"            -> "localhost",
    "kudu.catalog"           -> "geomesa",
    "geomesa.security.auths" -> "admin"
  )

  "KuduDataStore" should {

    "support table splitting" in {
      val typeName = "testsplits"

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KuduDataStore]
      ds must not(beNull)

      try {
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326;table.splitter.options=" +
              "'id.pattern:[A-Z],z3.min:2017-01-01,z3.max:2017-01-10,z3.bits:2,attr.name.pattern:[A-Z],z2.bits:2'"))
        val sft = ds.getSchema(typeName)
        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          sf.setAttribute(1, Int.box(i))
          sf.setAttribute(2, f"2014-01-${i + 1}%02dT00:00:01.000Z")
          sf.setAttribute(3, s"POINT(4$i 5$i)")
          sf
        }

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs(toAdd.map(_.getID))
        ds.removeSchema(typeName)

        ds.getSchema(typeName) must beNull
      } finally {
        ds.dispose()
      }
    }

    "work with points" in {
      val typeName = "testpoints"

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KuduDataStore]
      ds must not(beNull)

      try {
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326"))

        val sft = ds.getSchema(typeName)
        sft must not(beNull)

        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          sf.setAttribute(1, Int.box(i))
          sf.setAttribute(2, f"2014-01-${i + 1}%02dT00:00:01.000Z")
          sf.setAttribute(3, s"POINT(4$i 5$i)")
          val vis = i % 3 match {
            case 0 => null
            case 1 => "user|admin"
            case 2 => "admin"
          }
          SecurityUtils.setFeatureVisibility(sf, vis)
          sf
        }

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs(toAdd.map(_.getID))

        forall(Seq(null, Array.empty[String], Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "name < 'name5' AND abs(age) < 3", transforms, toAdd.take(3))
          testQuery(ds, typeName, "name = 'name5' OR name = 'name7'", transforms, Seq(toAdd(5), toAdd(7)))
          testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5), toAdd(6)))
        }

        def testTransforms(ds: KuduDataStore) = {
          val transforms = Array("derived=strConcat('hello',name)", "geom")
          forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
            val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
            val features = SelfClosingIterator(fr).map(ScalaSimpleFeature.copy).toList // copy features as the same one is mutated
            features.headOption.map(f => SimpleFeatureTypes.encodeType(f.getFeatureType)) must
                beSome("derived:String,*geom:Point:srid=4326")
            features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
            forall(features) { feature =>
              feature.getAttribute("derived") mustEqual s"helloname${feature.getID}"
              feature.getAttribute("geom") mustEqual results.find(_.getID == feature.getID).get.getAttribute("geom")
            }
          }
        }

        testTransforms(ds)

        ds.getFeatureSource(typeName).removeFeatures(ECQL.toFilter("INCLUDE"))

        forall(Seq("INCLUDE",
          "IN('0', '2')",
          "bbox(geom,42,48,52,62)",
          "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
          "(name = 'name5' OR name = 'name6') and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
          "name < 'name5'",
          "name = 'name5'")) { filter =>
            testQuery(ds, typeName, filter, null, Seq.empty)
        }

        ds.removeSchema(typeName)
        ds.getSchema(typeName) must beNull
      } finally {
        ds.dispose()
      }
    }

    "work with polys" in {
      val typeName = "testpolys"

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KuduDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Polygon:srid=4326"))

        val sft = ds.getSchema(typeName)
        sft must not(beNull)

        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
          sf.setAttribute(2, s"POLYGON((-120 4$i, -120 50, -125 50, -125 4$i, -120 4$i))")
          sf
        }

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

        testQuery(ds, typeName, "INCLUDE", null, toAdd)
        testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
        testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
        testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
        testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
        testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, Seq(toAdd(5), toAdd(6)))

        ds.removeSchema(typeName)
        ds.getSchema(typeName) must beNull
      } finally {
        ds.dispose()
      }
    }
  }

  def testQuery(ds: KuduDataStore,
                typeName: String,
                filter: String,
                transforms: Array[String],
                results: Seq[SimpleFeature],
                explain: Option[Explainer] = None): MatchResult[_] = {
    QueryProperties.ScanRangesTarget.threadLocalValue.set("10")
    try {
      val query = new Query(typeName, ECQL.toFilter(filter), transforms)
      explain.foreach(e => ds.getQueryPlan(query, explainer = e))
      val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(fr).map(ScalaSimpleFeature.copy).toList // copy features as the same one is mutated
      val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.map(_.getLocalName).toArray)
      features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
      forall(features) { feature =>
        feature.getAttributes must haveLength(attributes.length)
        forall(attributes.zipWithIndex) { case (attribute, i) =>
          feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
          feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
        }
      }
    } finally {
      QueryProperties.ScanRangesTarget.threadLocalValue.remove()
    }
  }
}


