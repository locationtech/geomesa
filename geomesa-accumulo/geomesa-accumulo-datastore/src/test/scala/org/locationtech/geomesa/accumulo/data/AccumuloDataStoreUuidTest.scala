/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.accumulo.core.security.Authorizations
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreUuidTest extends Specification with TestWithDataStore {

  import scala.collection.JavaConverters._

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  override val spec =
    s"name:String:index=true,age:Int:index=join,dtg:Date,*geom:Point:srid=4326;${Configs.FID_UUID_KEY}=true"

  val features = (0 until 10).map { i =>
    val id = s"28a12c18-e5ae-4c04-ae7b-bf7cdbfaf23$i"
    val name = s"name$i"
    val age = 20 + i / 2
    ScalaSimpleFeature.create(sft, id, name, age, s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  val toBytes = GeoMesaFeatureIndex.idToBytes(sft)
  val uuids = features.map(f => toBytes(f.getID))
  val uuidStrings = uuids.map(ByteArrays.toHex)

  step {
    addFeatures(features)
  }

  "AccumuloDataStore" should {
    "encode UUIDs as 16 bytes" in {
      foreach(uuids)(_ must haveLength(16))
      foreach(ds.getAllIndexTableNames(sftName)) { table =>
        WithClose(connector.createScanner(table, new Authorizations)) { scanner =>
          // compare the feature id serialized at the end of each row key
          val bytes = scanner.asScala.map(_.getKey.getRow.getBytes.takeRight(16)).toList
          // note: attribute table has each key twice, so can't use containTheSameElementsAs
          bytes.map(ByteArrays.toHex) must containAllOf(uuidStrings)
        }
      }
    }
    "fail when writing non-uuids" in {
      WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        val feature = writer.next()
        feature.setAttributes(features.head.getAttributes)
        feature.getUserData.put(Hints.PROVIDED_FID, "foo")
        writer.write must throwAn[IllegalArgumentException]
      }
    }
    "return correct ids" in {
      val filters = Seq(
        "IN ('28a12c18-e5ae-4c04-ae7b-bf7cdbfaf235')",
        "bbox(geom,35,55,45,65)",
        "bbox(geom,35,55,45,65) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T00:08:00.000Z",
        "name IN ('name1', 'name2')",
        "age = 24"
      ).map(ECQL.toFilter)
      val transforms = Seq(
        null,
        Array("age", "geom"),
        Array("name", "geom"),
        Array("dtg", "geom")
      )
      val ids = features.map(_.getID)
      foreach(filters) { filter =>
        foreach(transforms) { transform =>
          val query = new Query(sftName, filter, transform)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must not(beEmpty)
          foreach(result)(f => ids.contains(f.getID) must beTrue)
        }
      }
    }
    "still query by feature id" in {
      val query = new Query(sftName, ECQL.toFilter("IN ('28a12c18-e5ae-4c04-ae7b-bf7cdbfaf235')"))
      foreach(ds.getQueryPlan(query)) { plan =>
        plan.filter.index.name mustEqual IdIndex.name
      }
      val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      result mustEqual Seq(features(5))
    }
    "return minified arrow ids and use them for callbacks" in {
      val filter = "bbox(geom,35,55,45,65) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T00:08:00.000Z"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_PROXY_FID, true)
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name,age")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, false)
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)

      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      val ids = WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        WithClose(reader.features())(_.map(_.getID.toInt).toList)
      }
      val proxy = new ProxyIdFunction()
      features.map(proxy.evaluate(_)) must containAllOf(ids)

      val callback = new Query(sftName, ECQL.toFilter(s"$filter AND proxyId() = ${ids.head}"))
      val callbackResults = SelfClosingIterator(ds.getFeatureReader(callback, Transaction.AUTO_COMMIT)).toList
      callbackResults must haveLength(1)
      proxy.evaluate(callbackResults.head) mustEqual ids.head
    }
    "return multiple minified arrow ids and use them for callbacks" in {
      val filter = "bbox(geom,35,55,45,65) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T00:08:00.000Z"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_PROXY_FID, true)
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name,age")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, false)
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)

      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      val ids = WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        WithClose(reader.features())(_.map(_.getID.toInt).toList)
      }
      val proxy = new ProxyIdFunction()
      features.map(proxy.evaluate(_)) must containAllOf(ids)

      val callback = new Query(sftName, ECQL.toFilter(s"$filter AND (proxyId() = ${ids.head} OR proxyId() = ${ids.last})"))
      val callbackResults = SelfClosingIterator(ds.getFeatureReader(callback, Transaction.AUTO_COMMIT)).toList
      callbackResults must haveLength(2)
      proxy.evaluate(callbackResults.head) mustEqual ids.head
      proxy.evaluate(callbackResults.last) mustEqual ids.last
    }.pendingUntilFixed("GEOMESA-2562")
  }

  step {
    allocator.close()
  }
}
