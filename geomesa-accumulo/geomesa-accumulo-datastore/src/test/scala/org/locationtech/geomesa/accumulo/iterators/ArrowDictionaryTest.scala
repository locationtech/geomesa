/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

@RunWith(classOf[JUnitRunner])
class ArrowDictionaryTest extends TestWithFeatureType with Mockito with LazyLogging {

  import scala.collection.JavaConverters._

  override val spec = "team:String,name:String:index=true,age:Int,weight:Float,dtg:Date,*geom:Point:srid=4326;geomesa.stats.enable=false"

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"team${i % 2}", s"name$i", i, i * 2, s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  lazy val localDs =
    DataStoreFinder
        .getDataStore((dsParams ++ Map(AccumuloDataStoreParams.RemoteArrowParam.key -> "false")).asJava)
        .asInstanceOf[AccumuloDataStore]
  lazy val dataStores = Seq(ds, localDs)

  step {
    addFeatures(features)
  }

  "Arrow delta scans" should {
    "query with differently ordered dictionaries" in {
      val attributes = Seq("team","name","age","weight")
      val transforms =
        (2 to attributes.length)
            .reverse
            .foldLeft[Seq[Seq[String]]](Seq.empty)((seq, i) => seq ++ attributes.combinations(i))
            .map(_ ++ Seq("dtg", "geom"))
      val filter = ECQL.toFilter("bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z")
      dataStores.foreach { ds =>
        transforms.foreach { transform =>
          Seq(transform.dropRight(2).reverse, transform.dropRight(2)).distinct.foreach { dictionaries =>
            val query = new Query(sft.getTypeName, filter, transform: _*)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, dictionaries.mkString(","))
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            val result = WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              WithClose(reader.features())(_.map(ScalaSimpleFeature.copy).toList)
            }
            result.map(_.getID) mustEqual features.map(_.getID)
            result.map(_.getAttributes.asScala) mustEqual features.map(f => transform.map(f.getAttribute))
          }
        }
      }
      ok
    }
  }

  step {
    allocator.close()
  }
}
