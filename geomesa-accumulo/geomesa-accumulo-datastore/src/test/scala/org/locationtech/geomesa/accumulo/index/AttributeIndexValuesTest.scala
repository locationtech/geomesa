/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.BatchScanPlan
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.EnumerationStat
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttributeIndexValuesTest extends TestWithDataStore {

  override val spec = "name:String:index=join:index-value=true,age:Int:index=join," +
      "team:String:index-value=true,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"$i", s"team${i % 2}", s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  val filters = Seq(
    (ECQL.toFilter("name in ('name1', 'name2')"), Seq(1, 2)),
    (ECQL.toFilter("name = 'name1'"), Seq(1)),
    (ECQL.toFilter("age in (1, 2)"), Seq(1, 2)),
    (ECQL.toFilter("age = 1"), Seq(1))
  )

  val transforms = Seq(
    Array("name", "dtg", "geom"),
    Array("team", "dtg", "geom"),
    Array("name", "team", "dtg", "geom")
  )

  step {
    addFeatures(features)
  }

  "AttributeIndexValues" should {
    "work with stats queries" in {
      foreach(filters) { case (filter, expectation) =>
        foreach(Seq("name", "team")) { enumeration =>
          val query = new Query(sftName, filter)
          query.getHints.put(QueryHints.STATS_STRING, s"Enumeration('$enumeration')")
          query.getHints.put(QueryHints.ENCODE_STATS, true)
          foreach(ds.getQueryPlan(query)) { plan => plan must beAnInstanceOf[BatchScanPlan] }
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val stats = results.map(f => StatsScan.decodeStat(sft)(f.getAttribute(0).asInstanceOf[String])).toList
          stats must haveLength(1)
          stats.head must beAnInstanceOf[EnumerationStat[String]]
          stats.head.asInstanceOf[EnumerationStat[String]].property mustEqual enumeration
          val expected = expectation.map(i => features(i).getAttribute(enumeration)).groupBy(e => e).toSeq.map {
            case (e, list) => (e, list.length.toLong)
          }
          stats.head.asInstanceOf[EnumerationStat[String]].frequencies.toList must containTheSameElementsAs(expected)
        }
      }
    }
    "work with arrow queries" in {
      import scala.collection.JavaConverters._
      foreach(filters) { case (filter, expectation) =>
        foreach(transforms) { transform =>
          val dicts = transform.filter(t => t != "dtg" && t != "geom")
          val query = new Query(sftName, filter, transform)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, dicts.mkString(","))
          query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
          query.getHints.put(QueryHints.ARROW_INCLUDE_FID, true)
          foreach(ds.getQueryPlan(query)) { plan => plan must beAnInstanceOf[BatchScanPlan] }
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            SelfClosingIterator(reader.features()).map(_.getAttributes.asScala).toSeq must
                containTheSameElementsAs(expectation.map(i => transform.toSeq.map(features(i).getAttribute)))
          }
        }
      }
    }
  }

  step {
    allocator.close()
  }
}
