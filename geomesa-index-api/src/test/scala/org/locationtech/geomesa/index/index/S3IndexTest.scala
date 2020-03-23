/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.utils.ExplainPrintln
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class S3IndexTest extends Specification with LazyLogging {

  sequential

  val spec = "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=s3:geom:dtg"

  lazy val sft = SimpleFeatureTypes.createType("test", spec)

  lazy val ds = new TestGeoMesaDataStore(false) // requires strict bbox...

  lazy val features =
    (0 until 10).map { i =>
      ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(4$i 60)")
    } ++ (10 until 20).map { i =>
      ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2010-05-${i}T$i:00:00.000Z", s"POINT(4${i - 10} 60)")
    } ++ (20 until 30).map { i =>
      ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2010-05-${i}T${i-10}:00:00.000Z", s"POINT(6${i - 20} 60)")
    }

  step {
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  def execute(query: Query): Seq[SimpleFeature] =
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

  def execute(ecql: String, transforms: Option[Array[String]] = None, explain: Boolean = false): Seq[SimpleFeature] = {
    if (explain) {
      ds.getQueryPlan(new Query(sft.getTypeName, ECQL.toFilter(ecql), transforms.orNull), explainer = new ExplainPrintln)
    }
    execute(new Query(sft.getTypeName, ECQL.toFilter(ecql), transforms.orNull))
  }

  "S3Index" should {
    "return all features for inclusive filter" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
    }

    "return some features for exclusive geom filter" >> {
      val filter = "bbox(geom, 38, 59, 45, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(6)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
    }

    "return some features for exclusive date filter" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
    }

    "work with whole world filter" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T08:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(5 to 8)
    }

    "work across week bounds" >> {
      val filter = "bbox(geom, 45, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(9)
      features.map(_.getID.toInt) must containTheSameElementsAs((6 to 9) ++ (15 to 19))
    }

    "work across 2 weeks" >> {
      val filter = "bbox(geom, 44.5, 59, 50, 61)" +
          " AND dtg between '2010-05-10T00:00:00.000Z' and '2010-05-17T23:59:59.999Z'"
      val features = execute(filter)
      features must haveSize(3)
      features.map(_.getID.toInt) must containTheSameElementsAs(15 to 17)
    }

    "work with whole world filter across week bounds" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(15)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 20)
    }

    "work with whole world filter across 3 week periods" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg between '2010-05-08T06:00:00.000Z' and '2010-05-30T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(20)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 29)
    }

    "work with small bboxes and date ranges" >> {
      val filter = "bbox(geom, 40.999, 59.999, 41.001, 60.001)" +
          " AND dtg between '2010-05-07T00:59:00.000Z' and '2010-05-07T01:01:00.000Z'"
      val features = execute(filter)
      features must haveSize(1)
      features.head.getID.toInt mustEqual 1
    }

    "support AND'ed GT/LT for dates" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg >= '2010-05-07T06:00:00.000Z' AND dtg <= '2010-05-08T00:00:00.000Z'"
      val features = execute(filter)
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
    }

    "apply secondary filters" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T10:00:00.000Z'" +
          " AND name = 'name8'"
      val features = execute(filter)
      features must haveSize(1)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
    }

    "apply transforms" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = execute(filter, Some(Array("name")))
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features) { f =>
        f.getAttributeCount mustEqual 1
        f.getAttribute("name") must not(beNull)
      }
    }

    "apply functional transforms" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = execute(filter, Some(Array("derived=strConcat('my', name)")))
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features) { f =>
        f.getAttributeCount mustEqual 1
        f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d")
      }
    }
  }
}
