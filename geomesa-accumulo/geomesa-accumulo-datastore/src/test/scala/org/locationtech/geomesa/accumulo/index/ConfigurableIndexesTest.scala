/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ConfigurableIndexesTest extends Specification with TestWithDataStore {

  sequential

  override val spec = s"name:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='${Z3Index.name}'"

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(s"f-$i", sft)
    sf.setAttribute(0, s"name-$i")
    sf.setAttribute(1, s"2016-01-01T0$i:01:00.000Z")
    sf.setAttribute(2, s"POINT(4$i 5$i)")
    sf
  }
  addFeatures(features)

  "AccumuloDataStore" should {
    "only create the z3 index" >> {
      ds.tableOps.exists(Z3Index.getTableName(sft.getTypeName, ds)) must beTrue
      forall(AccumuloFeatureIndex.AllIndices.filter(i => i != Z3Index)) { i =>
        Try(i.getTableName(sft.getTypeName, ds)).map(ds.tableOps.exists).getOrElse(false) must beFalse
      }
    }

    "be able to use z3 for spatial queries" >> {
      val filter = "BBOX(geom,40,50,50,60)"
      val query = new Query(sftName, ECQL.toFilter(filter))
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must haveSize(10)
      results.map(_.getID) must containTheSameElementsAs((0 until 10).map(i => s"f-$i"))
    }

    "be able to use z3 for spatial ors" >> {
      val filter = "BBOX(geom,40,50,45,55) OR BBOX(geom,44,54,50,60) "
      val query = new Query(sftName, ECQL.toFilter(filter))
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must haveSize(10)
      results.map(_.getID) must containTheSameElementsAs((0 until 10).map(i => s"f-$i"))
    }

    "be able to use z3 for spatial and attribute ors" >> {
      val filter = "BBOX(geom,40,50,45,55) OR name IN ('name-6', 'name-7', 'name-8', 'name-9')"
      val query = new Query(sftName, ECQL.toFilter(filter))
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must haveSize(10)
      results.map(_.getID) must containTheSameElementsAs((0 until 10).map(i => s"f-$i"))
    }

    "add another empty index" >> {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      sft.setIndices(sft.getIndices :+ (Z2Index.name, Z2Index.version, IndexMode.ReadWrite))
      ds.updateSchema(sftName, sft)
      forall(Seq(Z3Index, Z2Index))(i => ds.tableOps.exists(i.getTableName(sft.getTypeName, ds)) must beTrue)
      forall(AccumuloFeatureIndex.AllIndices.filter(i => i != Z3Index && i != Z2Index)) { i =>
        Try(i.getTableName(sft.getTypeName, ds)).map(ds.tableOps.exists).getOrElse(false) must beFalse
      }
      val scanner = connector.createScanner(Z2Index.getTableName(sft.getTypeName, ds), new Authorizations)
      try {
        scanner.iterator.hasNext must beFalse
      } finally {
        scanner.close()
      }
    }

    "use another index" >> {
      val filter = "BBOX(geom,40,50,51,61)"
      val query = new Query(sftName, ECQL.toFilter(filter))
      var results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must beEmpty

      val sf = new ScalaSimpleFeature(s"f-10", ds.getSchema(sftName))
      sf.setAttribute(0, "name-10")
      sf.setAttribute(1, "2016-01-01T10:01:00.000Z")
      sf.setAttribute(2, "POINT(50 60)")
      addFeatures(Seq(sf))

      results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must haveSize(1)
      results.head.getID mustEqual "f-10"
    }

    "use the original index" >> {
      val filter = "BBOX(geom,40,50,51,61) AND dtg DURING 2016-01-01T00:00:00.000Z/2016-01-02T00:00:00.000Z"
      val query = new Query(sftName, ECQL.toFilter(filter))
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must haveSize(11)
      results.map(_.getID) must containTheSameElementsAs((0 until 11).map(i => s"f-$i"))
    }

    "throw an exception if the indices are not valid" >> {
      val schema = "*geom:LineString:srid=4326;geomesa.indices.enabled="
      forall(Seq("z2", "xz3", "z3", "attr", "xz2,xz3", "foo")) { enabled =>
        ds.createSchema(SimpleFeatureTypes.createType(sft.getTypeName + "_fail", s"$schema'$enabled'")) must
          throwAn[IllegalArgumentException]
      }
    }
  }
}
