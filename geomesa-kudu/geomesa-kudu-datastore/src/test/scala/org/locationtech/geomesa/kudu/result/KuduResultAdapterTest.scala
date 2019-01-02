/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.result

import java.util.Date

import org.apache.kudu.client.RowResult
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.opengis.filter.Filter
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KuduResultAdapterTest extends Specification with Mockito {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val schema = KuduSimpleFeatureSchema(sft)

  val arrowHints = new Hints
  arrowHints.put(QueryHints.ARROW_ENCODE, true)
  arrowHints.put(QueryHints.ARROW_INCLUDE_FID, true)
  arrowHints.put(QueryHints.ARROW_PROXY_FID, true)
  arrowHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
  arrowHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
  arrowHints.put(QueryHints.ARROW_SORT_REVERSE, false)
  arrowHints.put(QueryHints.ARROW_DICTIONARY_FIELDS , "name,age")

  val binHints = new Hints
  binHints.put(QueryHints.BIN_TRACK, "name")
  binHints.put(QueryHints.BIN_GEOM, "geom")

  val densityHints = new Hints
  densityHints.put(QueryHints.DENSITY_BBOX, new ReferencedEnvelope(-180, 180, -90, 90, CRS_EPSG_4326))
  densityHints.put(QueryHints.DENSITY_WIDTH, 640)
  densityHints.put(QueryHints.DENSITY_HEIGHT, 480)

  val statsHints = new Hints
  statsHints.put(QueryHints.STATS_STRING, "MinMax(name)")
  statsHints.put(QueryHints.ENCODE_STATS, true)

  "KuduResultAdapter" should {

    "serialize adapters" in {
      foreach(Seq(null, Array("dtg", "geom"))) { transform =>
        foreach(Seq(None, Some(ECQL.toFilter("name = 'foo'")))) { ecql =>
          foreach(Seq(Seq.empty, Seq("user".getBytes, "admin".getBytes))) { auths =>
            foreach(Seq(new Hints, arrowHints, binHints, densityHints, statsHints)) { hints =>
              val query = new Query("test", ecql.getOrElse(Filter.INCLUDE), transform)
              query.getHints.asInstanceOf[java.util.Map[AnyRef, AnyRef]].putAll(hints)
              QueryPlanner.setQueryTransforms(query, sft)
              val adapter = KuduResultAdapter(sft, auths, ecql, query.getHints)
              KuduResultAdapter.deserialize(KuduResultAdapter.serialize(adapter)).toString mustEqual adapter.toString
            }
          }
        }
      }
    }

    "adapt features directly" in {
      val adapter = KuduResultAdapter(sft, Seq.empty, None, new Hints)
      adapter must beAnInstanceOf[DirectAdapter]

      val result = mock(Some("fid"), Some("name"), Some(21),
        Some(Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])), Some((45d, 55d)))

      val adapted = adapter.adapt(Iterator.single(result))
      adapted.hasNext must beTrue
      adapted.next mustEqual ScalaSimpleFeature.create(sft, "fid", "name", 21, "2018-01-01T00:00:00.000Z", "POINT (45 55)")
      adapted.hasNext must beFalse
    }

    "adapt features with filters" in {
      val adapter = KuduResultAdapter(sft, Seq.empty, Some(ECQL.toFilter("name = 'name0'")), new Hints)
      adapter must beAnInstanceOf[FilteringAdapter]

      val result0 = mock(Some("fid0"), Some("name0"), Some(21),
        Some(Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])), Some((45d, 55d)))
      val result1 = mock(Some("fid1"), Some("name1"), Some(22),
        Some(Converters.convert("2018-01-01T01:00:00.000Z", classOf[Date])), Some((46d, 56d)))

      val adapted = adapter.adapt(Iterator(result0, result1))
      adapted.hasNext must beTrue
      adapted.next mustEqual ScalaSimpleFeature.create(sft, "fid0", "name0", 21, "2018-01-01T00:00:00.000Z", "POINT (45 55)")
      adapted.hasNext must beFalse
    }

    "adapt features with transforms" in {
      val props = Array("name", "geom")
      val query = new Query(sft.getTypeName, Filter.INCLUDE, props)
      QueryPlanner.setQueryTransforms(query, sft)

      val adapter = KuduResultAdapter(sft, Seq.empty, None, query.getHints)
      adapter must beAnInstanceOf[TransformAdapter]

      val result = mock(Some("fid0"), Some("name0"), None, None, Some((45d, 55d)))

      val adapted = adapter.adapt(Iterator.single(result))
      adapted.hasNext must beTrue
      adapted.next mustEqual ScalaSimpleFeature.create(DataUtilities.createSubType(sft, props), "fid0", "name0", "POINT (45 55)")
      adapted.hasNext must beFalse
    }

    "adapt features with filter and transforms" in {
      val props = Array("geom")
      val query = new Query(sft.getTypeName, Filter.INCLUDE, props)
      QueryPlanner.setQueryTransforms(query, sft)

      val adapter = KuduResultAdapter(sft, Seq.empty, Some(ECQL.toFilter("name = 'name0'")), query.getHints)
      adapter must beAnInstanceOf[FilteringTransformAdapter]

      val result0 = mock(Some("fid0"), Some("name0"), None, None, Some((45d, 55d)))
      val result1 = mock(Some("fid1"), Some("name1"), None, None, Some((46d, 56d)))

      val adapted = adapter.adapt(Iterator(result0, result1))
      adapted.hasNext must beTrue
      adapted.next mustEqual ScalaSimpleFeature.create(DataUtilities.createSubType(sft, props), "fid0", "POINT (45 55)")
      adapted.hasNext must beFalse
    }
  }

  def mock(fid: Option[String], name: Option[String], age: Option[Int], dtg: Option[Date], geom: Option[(Double, Double)]): RowResult = {
    val result = mock[RowResult]
    fid.foreach(result.getString(FeatureIdAdapter.name) returns _)
    result.isNull(schema.schema(Seq("name")).head.getName) returns name.isEmpty
    name.foreach(result.getString(schema.schema(Seq("name")).head.getName) returns _)
    result.isNull(schema.schema(Seq("age")).head.getName) returns age.isEmpty
    age.foreach(result.getInt(schema.schema(Seq("age")).head.getName) returns _)
    result.isNull(schema.schema(Seq("dtg")).head.getName) returns dtg.isEmpty
    dtg.foreach(d => result.getLong(schema.schema(Seq("dtg")).head.getName) returns d.getTime * 1000)
    result.isNull(schema.schema(Seq("geom")).head.getName) returns geom.isEmpty
    geom.foreach { case (x, y) =>
      result.getDouble(schema.schema(Seq("geom")).head.getName) returns x
      result.getDouble(schema.schema(Seq("geom")).last.getName) returns y
    }
    result.isNull(VisibilityAdapter.name) returns true
    result
  }
}
