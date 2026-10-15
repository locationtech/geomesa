/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.geotools.api.filter.Filter
import org.geotools.api.filter.MultiValuedFilter.MatchAction
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.jdbc.JDBCDataStore
import org.geotools.referencing.CRS
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.index.process.ArrowVisitor
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils

import java.io.{ByteArrayInputStream, SequenceInputStream}
import java.util.concurrent._
import java.util.logging.{Handler, Level, LogRecord}
import java.util.{Collections, Locale}

class PartitionedPostgisDataStoreQueryTest extends TestWithSharedPostgisContainer {

  import scala.collection.JavaConverters._

  "PartitionedPostgisDataStore" should {

    "filter on list elements" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "list-filters") { sft =>
          val features = writeFeatures(ds, sft)
          val fif = FilterHelper.ff
          val filters = Seq(
            fif.equals(fif.property("name"), fif.literal("name0")),
            fif.equal(fif.property("name"), fif.literal("name0"), false, MatchAction.ANY),
            fif.equals(fif.property("name"), fif.literal(Collections.singletonList("name0"))),
            fif.equal(fif.property("name"), fif.literal(Collections.singletonList("name0")), false, MatchAction.ANY),
            fif.equal(fif.property("name"), fif.literal(java.util.List.of("name0", "alt0")), false, MatchAction.ANY),
            fif.equal(fif.property("name"), fif.literal(java.util.List.of("name0", "alt0")), false, MatchAction.ALL),
            ECQL.toFilter("name = 'name0'"),
          )
          foreach(filters) { filter =>
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result must haveLength(1)
              compFromDb(result.head) mustEqual compWithFid(features.head, sft)
            }
          }

          val orFilters = Seq(
            ECQL.toFilter("name IN('name0','name1')"),
            ECQL.toFilter("name = 'name0' OR name = 'name1'"),
          )
          foreach(orFilters) { filter =>
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result must haveLength(2)
              result.sortBy(_.getID).map(compFromDb) mustEqual features.take(2).map(compWithFid(_, sft))
            }
          }

          val nonMatchingFilters = Seq(
            fif.equal(fif.property("name"), fif.literal("name0"), false, MatchAction.ALL),
            fif.equal(fif.property("name"), fif.literal(Collections.singletonList("name0")), false, MatchAction.ALL),
            ECQL.toFilter("name = 'name0' AND name = 'name1'"),
          )
          foreach(nonMatchingFilters) { filter =>
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result must beEmpty
            }
          }
        }
      }
    }

    "filter on json elements" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "json-filters") { sft =>
          val features = writeFeatures(ds, sft)
          val filters = Seq(
            ECQL.toFilter("jsonPath('$.props.names[0]') = 'name0'") -> features.take(1),
            ECQL.toFilter("\"$.props.names[0]\" = 'name0'") -> features.take(1),
            ECQL.toFilter("jsonPath(props, 'age') > 5.0") -> features.drop(6),
            ECQL.toFilter("\"$.props.age\" > 5.0") -> features.drop(6),
            ECQL.toFilter("\"$.props.age\" = 5.0 OR \"$.props.age\" = 6.0 ") -> features.slice(5, 7),
          )
          foreach(filters) { case (filter, expected) =>
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList.sortBy(_.getID)
              result must haveLength(expected.length)
              result.map(compFromDb) mustEqual expected.map(compWithFid(_, sft))
            }
          }
        }
      }
    }

    "run arrow queries with dictionary encoded list attributes" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "list-arrow") { sft =>
          val features = writeFeatures(ds, sft)

          val arrowVisitor =
            new ArrowVisitor(ds.getSchema(sft.getTypeName), SimpleFeatureEncoding.min(includeFids = true).copy(date = Encoding.Max), "18.3.0",
              Seq("name"), Some("dtg"), Some(true), preSorted = false, 100, flattenStruct = false)

          ds.getFeatureSource(sft.getTypeName).getFeatures.accepts(arrowVisitor, null)

          val is = new SequenceInputStream(arrowVisitor.getResult.results.asScala.map(new ByteArrayInputStream(_)).asJavaEnumeration)
          WithClose(SimpleFeatureArrowFileReader.streaming(is)) { reader =>
            WithClose(reader.features())(_.map(compFromDb).toList) mustEqual features.map(compWithFid(_, sft))
          }
        }
      }
    }

    "remove whole-world filters" in {
      val wholeWorldFilters = {
        // note: can't use ECQL.toFilter as it tries to load jai and dies
        val fif = FilterHelper.ff
        val geom = fif.property("geom")
        val bbox = fif.bbox(geom, -180, -90, 180, 90, CRS.toSRS(CRS_EPSG_4326))
        val intersects =
          fif.intersects(geom, fif.literal(WKTUtils.read("POLYGON((-190 -100, 190 -100, 190 100, -190 100, -190 -100))")))
        Seq(bbox, intersects)
      }

      WithClose(datastore()) { ds =>
        foreach(Seq(true, false)) { ignoreFilters =>
          testSchema(ds, s"ignore_$ignoreFilters", s"$spec,${SftUserData.FilterWholeWorld.key}=$ignoreFilters") { sft =>
            val features = writeFeatures(ds, sft)
            foreach(wholeWorldFilters) { filter =>
              val Array(left, right) = ds.dialect.splitFilter(filter, ds.getSchema(sft.getTypeName))
              if (ignoreFilters) {
                left mustEqual Filter.INCLUDE
              } else {
                left mustEqual filter
              }
              right mustEqual Filter.INCLUDE

              // track the messages logged by the JDBC store to verify the filter being run
              val messages = new CopyOnWriteArrayList[(String, String)]()
              val threadId = Thread.currentThread().getId
              val handler = new Handler() {
                override def publish(record: LogRecord): Unit =
                  if (record.getThreadID == threadId) { messages.add((record.getSourceMethodName, record.getMessage)) }
                override def flush(): Unit = {}
                override def close(): Unit = {}
              }
              val logger = ds.unwrap(classOf[JDBCDataStore]).getLogger
              logger.setLevel(Level.FINE)
              logger.addHandler(handler)
              WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
                val result = CloseableIterator(reader).toList
                result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
              }

              val selects = messages.asScala.collect { case ("selectSQLPS", v) => v }
              selects must haveLength(1)
              if (ignoreFilters) {
                selects.head.toLowerCase(Locale.US) must not(contain("where"))
              } else {
                selects.head.toLowerCase(Locale.US) must contain("where")
              }
            }
          }
        }
      }
    }

    "group multiple feature ids when combined with another predicate" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "id-predicate-grouping") { sft =>
          writeFeatures(ds, sft)
          val ids = new java.util.HashSet[org.geotools.api.filter.identity.FeatureId]()
          ids.add(new FeatureIdImpl("fid0"))
          ids.add(new FeatureIdImpl("fid9"))
          val fif = FilterHelper.ff
          val filter = fif.and(
            fif.id(ids),
            fif.lessOrEqual(fif.property("dtg"), fif.literal(new java.util.Date(now - 60 * 60 * 1000))))

          WithClose(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)) { reader =>
            CloseableIterator(reader).map(_.getID).toList mustEqual Seq(s"${sft.getTypeName}.fid9")
          }
        }
      }
    }

    "support query interceptors" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "interceptor", s"$spec,${SimpleFeatureTypes.Configs.QueryInterceptors}=${classOf[TestQueryInterceptor].getName}") { sft =>
          val features = writeFeatures(ds, sft)

          val Array(left, right) = ds.dialect.splitFilter(Filter.EXCLUDE, sft)
          left mustEqual Filter.INCLUDE
          right mustEqual Filter.INCLUDE

          // verify that filter is re-written to be Filter.INCLUDE
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("IN('1')")), Transaction.AUTO_COMMIT)) { reader =>
            val result = CloseableIterator(reader).toList
            result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
          }
        }
      }
    }
  }
}
