/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.filter.FilterList
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{CoprocessorPlan, ScanPlan}
import org.locationtech.geomesa.hbase.rpc.filter.Z3HBaseFilter
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class HBaseArrowTest extends Specification with LazyLogging  {

  import scala.collection.JavaConverters._

  lazy val sft = SimpleFeatureTypes.createType("arrow", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"${i % 5}", s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  lazy val params = Map(
    HBaseDataStoreParams.ConnectionParam.getName   -> MiniCluster.connection,
    HBaseDataStoreParams.HBaseCatalogParam.getName -> HBaseArrowTest.this.getClass.getSimpleName
  )

  lazy val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
  lazy val dsSemiLocal = DataStoreFinder.getDataStore((params ++ Map(HBaseDataStoreParams.ArrowCoprocessorParam.key -> false)).asJava).asInstanceOf[HBaseDataStore]
  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsThreads1 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "1")).asInstanceOf[HBaseDataStore]
  lazy val dsYieldPartials = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.YieldPartialResultsParam.key -> true)).asInstanceOf[HBaseDataStore]
  lazy val dataStores = Seq(ds, dsSemiLocal, dsFullLocal, dsThreads1, dsYieldPartials)

  step {
    logger.info("Starting HBase Arrow Test")
    ds.createSchema(sft)
    val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
    features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    writer.close()
  }

  "HBaseDataStoreFactory" should {
    "enable coprocessors" in {
      ds.config.remoteFilter must beTrue
      ds.config.coprocessors.enabled.arrow must beTrue
      dsSemiLocal.config.remoteFilter must beTrue
      dsSemiLocal.config.coprocessors.enabled.arrow must beFalse
      dsFullLocal.config.remoteFilter must beFalse
    }
  }

  "ArrowFileCoprocessor" should {
    "return arrow dictionary encoded data" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name,age")
        query.getHints.put(QueryHints.ARROW_MULTI_FILE, true)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          // sft name gets dropped, so we can't compare directly
          SelfClosingIterator(reader.features()).map(f => (f.getID, f.getAttributes)).toSeq must
              containTheSameElementsAs(features.map(f => (f.getID, f.getAttributes)))
        }
      }
    }
  }

  "ArrowBatchCoprocessor" should {
    "return arrow encoded data" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(features)
        }
      }
    }
    "return arrow dictionary encoded data" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name,age")
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(features)
        }
      }
    }
    "return arrow dictionary encoded data with provided dictionaries" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, "name,name0")
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          val expected = features.map {
            case f if f.getAttribute(0) != "name1" => f
            case f =>
              val e = ScalaSimpleFeature.copy(sft, f)
              e.setAttribute(0, "[other]")
              e
          }
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(expected)
        }
      }
    }
    "return arrow encoded projections" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE, Array("dtg", "geom"))
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(_.getAttributes.asScala).toSeq must
              containTheSameElementsAs(features.map(f => List(f.getAttribute("dtg"), f.getAttribute("geom"))))
        }
      }
    }
    "return sorted batches" in {
      // TODO figure out how to test multiple batches (client side merge)
      val filters = Seq(
        "INCLUDE" -> features,
        "IN ('0', '1', '2')" -> features.take(3),
        "bbox(geom,38,65.5,42,69.5)" -> features.slice(6, 10),
        "bbox(geom,38,65.5,42,69.5) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T00:08:00.000Z" -> features.slice(6, 8)
      )
      val transforms = Seq(Array("name", "dtg", "geom"), null)
      foreach(dataStores) { ds =>
        foreach(filters) { case (ecql, expected) =>
          foreach(transforms) { transform =>
            val query = new Query(sft.getTypeName, ECQL.toFilter(ecql), transform)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              if (transform == null) {
                SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList mustEqual expected
              } else {
                SelfClosingIterator(reader.features()).map(f => f.getID -> f.getAttributes.asScala).toList mustEqual
                    expected.map(f => f.getID -> transform.map(f.getAttribute).toSeq)
              }
            }
          }
        }
      }
    }
    "return sampled arrow encoded data" in {
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.SAMPLING, 0.2f)
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          val results = SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq
          results.length must beLessThan(10)
          foreach(results)(features must contain(_))
        }
      }
    }
    "return arrow dictionary encoded data without caching and with z-values" in {
      val filter = ECQL.toFilter("bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z")
      foreach(dataStores) { ds =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, java.lang.Boolean.FALSE)
        query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 5)
        foreach(ds.getQueryPlan(query)) { plan =>
          if (ds.config.remoteFilter) {
            if (ds.config.coprocessors.enabled.arrow) {
              plan must beAnInstanceOf[CoprocessorPlan]
            } else {
              plan must beAnInstanceOf[ScanPlan]
            }
            plan.scans.head.scans.head.getFilter must beAnInstanceOf[FilterList]
            val filters = plan.scans.head.scans.head.getFilter.asInstanceOf[FilterList].getFilters
            filters.asScala.map(_.getClass) must contain(classOf[Z3HBaseFilter])
          } else {
            plan must beAnInstanceOf[ScanPlan]
          }
        }
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(features.filter(filter.evaluate))
        }
      }
    }
  }

  step {
    logger.info("Cleaning up HBase Arrow Test")
    dataStores.foreach { _.dispose() }
  }
}
