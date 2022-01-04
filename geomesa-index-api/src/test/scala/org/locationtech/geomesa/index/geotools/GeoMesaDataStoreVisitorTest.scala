/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.util.Collections

import org.geotools.data.{DataStore, Query}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.{ReTypingFeatureCollection, ReprojectingFeatureCollection}
import org.geotools.data.util.NullProgressListener
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.collection.{DecoratingFeatureCollection, DecoratingSimpleFeatureCollection}
import org.geotools.feature.visitor._
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreVisitorTest.{TestFeatureCollection, TestProgressListener, TestSimpleFeatureCollection, TestVisitor}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaDataStoreVisitorTest extends Specification {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  val ds = new TestGeoMesaDataStore(true)
  ds.createSchema(sft)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
  }

  val epsg3857 = CRS.decode("EPSG:3857")

  step {
    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
    ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))
  }

  "GeoMesaDataStore" should {
    "unwrap decorating feature collections" in {
      val fc = ds.getFeatureSource(sft.getTypeName).getFeatures()
      val collections = Seq(
        new ReTypingFeatureCollection(fc, SimpleFeatureTypes.renameSft(sft, "foo")),
        new ReprojectingFeatureCollection(fc, epsg3857),
        new TestFeatureCollection(fc),
        new TestSimpleFeatureCollection(fc)
      )
      foreach(collections) { collection =>
        val visitor = new TestVisitor()
        GeoMesaFeatureCollection.visit(collection, visitor)
        visitor.visited must beFalse
        visitor.executed must beTrue
      }
    }
    "optimize average visitors" in {
      val visitor = new AverageVisitor("age", sft)
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      visitor.getResult.getValue mustEqual
          (features.map(_.getAttribute("age").asInstanceOf[Int]).sum.toDouble / features.length)
      listener.warning must beNone
    }
    "optimize bounds visitors" in {
      val visitor = new BoundsVisitor()
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      val expected = new Envelope()
      features.foreach(f => expected.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal))
      visitor.getResult.getValue mustEqual expected
      listener.warning must beNone
    }
    "optimize count visitors" in {
      val visitor = new CountVisitor()
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      visitor.getCount mustEqual features.size
      listener.warning must beNone
    }
    "optimize max visitors" in {
      val visitor = new MaxVisitor("age", sft)
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      visitor.getResult.getValue mustEqual 9
      listener.warning must beNone
    }
    "optimize groupBy count visitors" in {
      val prop = CommonFactoryFinder.getFilterFactory2.property("age")
      val listener = new TestProgressListener()
      val visitor = new GroupByVisitor(Aggregate.COUNT, prop, Collections.singletonList(prop), listener)
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      val result = visitor.getResult.getValue
      result must beAnInstanceOf[Array[Array[_]]]
      result.asInstanceOf[Array[Array[_]]] must haveLength(features.size)
      result.asInstanceOf[Array[Array[_]]].toSeq.map(_.toSeq) must
          containTheSameElementsAs(features.map(f => Seq(f.getAttribute("age"), 1)))
      listener.warning must beNone
    }
    "optimize groupBy max visitors" in {
      val prop = CommonFactoryFinder.getFilterFactory2.property("age")
      val listener = new TestProgressListener()
      val visitor = new GroupByVisitor(Aggregate.MAX, prop, Collections.singletonList(prop), listener)
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      val result = visitor.getResult.getValue
      result must beAnInstanceOf[Array[Array[_]]]
      result.asInstanceOf[Array[Array[_]]] must haveLength(features.size)
      result.asInstanceOf[Array[Array[_]]].toSeq.map(_.toSeq) must
          containTheSameElementsAs(features.map(f => Seq(f.getAttribute("age"), f.getAttribute("age"))))
      listener.warning must beNone
    }
    "optimize groupBy min visitors" in {
      val prop = CommonFactoryFinder.getFilterFactory2.property("age")
      val listener = new TestProgressListener()
      val visitor = new GroupByVisitor(Aggregate.MIN, prop, Collections.singletonList(prop), listener)
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      val result = visitor.getResult.getValue
      result must beAnInstanceOf[Array[Array[_]]]
      result.asInstanceOf[Array[Array[_]]] must haveLength(features.size)
      result.asInstanceOf[Array[Array[_]]].toSeq.map(_.toSeq) must
          containTheSameElementsAs(features.map(f => Seq(f.getAttribute("age"), f.getAttribute("age"))))
      listener.warning must beNone
    }
    "optimize min visitors" in {
      val visitor = new MinVisitor("age", sft)
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      visitor.getResult.getValue mustEqual 0
      listener.warning must beNone
    }
    "optimize sum visitors" in {
      val visitor = new SumVisitor("age", sft)
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      visitor.getResult.getValue mustEqual features.map(_.getAttribute("age").asInstanceOf[Int]).sum
      listener.warning must beNone
    }
    "optimize unique visitors" in {
      val visitor = new UniqueVisitor("name", sft)
      val listener = new TestProgressListener()
      ds.getFeatureSource(sft.getTypeName).getFeatures().accepts(visitor, listener)
      val result = visitor.getResult.getValue
      result must beAnInstanceOf[java.util.Set[AnyRef]]
      result.asInstanceOf[java.util.Set[AnyRef]].asScala mustEqual features.map(_.getAttribute("name")).toSet
      listener.warning must beNone
    }
  }
}


object GeoMesaDataStoreVisitorTest {

  class TestQueryInterceptor extends QueryInterceptor {
    override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {}
    override def rewrite(query: Query): Unit =
      if (query.getFilter == Filter.INCLUDE) { query.setFilter(Filter.EXCLUDE) }
    override def close(): Unit = {}
  }

  class TestVisitor extends GeoMesaProcessVisitor {
    var executed = false
    var visited = false
    override def execute(source: SimpleFeatureSource, query: Query): Unit = executed = true
    override def visit(feature: Feature): Unit = visited = true
    override def getResult: CalcResult = null
  }

  class TestProgressListener extends NullProgressListener {
    var warning: Option[(String, String, String)] = None
    override def warningOccurred(source: String, location: String, warning: String): Unit = {
      this.warning = Some((source, location, warning))
    }
  }

  // example class extending DecoratingFeatureCollection
  class TestFeatureCollection(delegate: SimpleFeatureCollection)
      extends DecoratingFeatureCollection[SimpleFeatureType, SimpleFeature](delegate)

  // example class extending DecoratingSimpleFeatureCollection
  class TestSimpleFeatureCollection(delegate: SimpleFeatureCollection)
      extends DecoratingSimpleFeatureCollection(delegate)
}
