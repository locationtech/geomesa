/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStoreParams.ReadAccessRoles
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.{SftUserData, VisCol}
import org.locationtech.geomesa.gt.partition.postgis.dialect._
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.security.{AuthProviderParam, AuthorizationsProvider, SecurityUtils, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.matcher.MatchResult

import java.io.IOException
import java.util.Locale

class PartitionedPostgisDataStoreSecurityTest extends TestWithSharedPostgisContainer {

  import scala.collection.JavaConverters._

  "PartitionedPostgisDataStore" should {

    "read and write rows with visibilities" in {
      // noinspection TypeAnnotation
      val provider = new AuthorizationsProvider {
        var auths: Seq[String] = Seq.empty
        override def getAuthorizations: java.util.List[String] = auths.asJava
        override def configure(params: java.util.Map[String, _]): Unit = {}
      }
      WithClose(datastore(params ++ Map(AuthProviderParam.key -> provider))) { ds =>
        testSchema(ds, "vis_hidden", s"$spec,pg.vis.enabled=true") { sft =>
          sft.indexOf(PartitionedPostgisDialect.VisCol) mustEqual -1

          // verify the underlying tables have the vis col
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.getMetaData.getColumns(null, this.schema, s"${sft.getTypeName}_wa", null)) { rs =>
              Iterator.continually(rs).takeWhile(_.next()).map(_.getString("COLUMN_NAME")).toList must
                contain(PartitionedPostgisDialect.VisCol)
            }
            // and that the view filters on it
            WithClose(cx.prepareStatement(s"""select pg_get_viewdef('${this.schema}."${sft.getTypeName}"'::regclass, true);""")) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                rs.getString(1).toLowerCase(Locale.US).split("pg_vis") must haveLength(5) // 4 tables
              }
            }
          }

          // visibility label to assign to each feature
          val visibilities = Seq("admin", "user", "user&admin", "user|admin", "admin", "user", "user&admin", "user|admin", "admin", "user")
          val features = this.features(sft).zip(visibilities).map { case (sf, vis) => SecurityUtils.setFeatureVisibility(sf, vis) }

          // write some data
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              features.take(5).foreach { feature =>
                FeatureUtils.write(writer, feature, useProvidedFid = true)
              }
            }
            // validate adding through the feature store
            ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.drop(5).asJava))
            tx.commit()
          }

          def runQueries(): MatchResult[_] = {
            foreach(Seq(Seq.empty, Seq("admin"), Seq("user"), Seq("user", "admin"))) { auths =>
              provider.auths = auths
              val expected = features.filter { f =>
                val vis = SecurityUtils.getVisibility(f)
                // note: not a very robust check but works for our test data here
                if (vis.contains("|")) {
                  vis.split("\\|").exists(auths.contains)
                } else if (vis.contains("&")) {
                  vis.split("&").forall(auths.contains)
                } else {
                  auths.contains(vis)
                }
              }
              foreach(Seq(null, Array("name", "dtg", "geom"), Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
                val query = new Query(sft.getTypeName, Filter.INCLUDE, transforms: _*)
                val visible = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
                visible.map(compFromDb) mustEqual expected.map(compWithFid(_, sft, Option(transforms)))
                visible.map(f => VisibilityUtils.normalize(SecurityUtils.getVisibility(f))) mustEqual
                  expected.map(f => VisibilityUtils.normalize(SecurityUtils.getVisibility(f)))
                val fromFeatureSource =
                  CloseableIterator(ds.getFeatureSource(query.getTypeName).getFeatures(query).features()).toList.sortBy(_.getID)
                fromFeatureSource mustEqual visible
              }
            }
          }

          runQueries()

          // verify vis still work through maintenance scripts
          val typeInfo = TypeInfo(this.schema, sft)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            // everything starts in the write ahead log
            count(cx, typeInfo.tables.writeAhead) mustEqual 10
            WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
            // all rows must have moved out of the write ahead table into partitions - none dropped
            count(cx, typeInfo.tables.writeAhead) mustEqual 0
            val partitioned =
              count(cx, typeInfo.tables.writeAheadPartitions) + count(cx, typeInfo.tables.mainPartitions) +
                count(cx, typeInfo.tables.spillPartitions)
            partitioned mustEqual 10
          }

          runQueries()

          // run an upgrade command and verify vis are still enforced
          sft.getUserData.put(SftUserData.FilterWholeWorld.key, "false")
          ds.upgrade(sft)

          runQueries()

          // verify features without vis throw errors on write
          WithClose(new DefaultTransaction()) { tx =>
            val sf = ScalaSimpleFeature.copy(features.head)
            sf.getUserData.remove(SecurityUtils.FEATURE_VISIBILITY)
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              FeatureUtils.write(writer, sf, useProvidedFid = true) must throwAn[IllegalArgumentException]
            }
            val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
            fs.addFeatures(new ListFeatureCollection(sft, sf)) must throwAn[IllegalArgumentException]
            tx.commit()
          }

          // verify that if somehow features end up without vis, they are not returned
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            count(cx, typeInfo.tables.view) mustEqual 10
            Seq(typeInfo.tables.writeAheadPartitions, typeInfo.tables.mainPartitions, typeInfo.tables.spillPartitions).foreach { table =>
              val sql = s"""update ${table.name.quoted} set "$VisCol" = null where ${typeInfo.cols.fid.quoted} = '${features.head.getID}'"""
              WithClose(cx.prepareCall(sql))(_.executeUpdate())
            }
            count(cx, typeInfo.tables.view) mustEqual 9
          }
        }
      }
    }

    "support read-only roles" in {
      val readOnlyUser = "readme"
      val noAccessUser = "noread"

      WithClose(datastore(params ++ Map(ReadAccessRoles.key -> readOnlyUser))) { ds =>
        WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
          WithClose(cx.createStatement()) { st =>
            st.execute(s"CREATE ROLE $readOnlyUser WITH LOGIN PASSWORD '$readOnlyUser';")
            st.execute(s"CREATE ROLE $noAccessUser WITH LOGIN PASSWORD '$noAccessUser';")
          }
        }
        testSchema(ds, "readonly") { sft =>
          val features = writeFeatures(ds, sft)

          WithClose(DataStoreFinder.getDataStore((params ++ Map("user" -> noAccessUser, "passwd" -> noAccessUser)).asJava)) { noAccessDs =>
            noAccessDs.getSchema(sft.getTypeName) must throwAn[IOException]
            noAccessDs.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT) must throwAn[IOException]
            noAccessDs.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT) must throwAn[IOException]
          }
          WithClose(DataStoreFinder.getDataStore((params ++ Map("user" -> readOnlyUser, "passwd" -> readOnlyUser)).asJava)) { readOnlyDs =>
            // ensure we can't write data
            WithClose(new DefaultTransaction()) { tx =>
              val writer = readOnlyDs.getFeatureWriterAppend(sft.getTypeName, tx)
              val builder = new SimpleFeatureBuilder(sft)
              builder.init(features.head)
              val feature = builder.buildFeature(s"fidxx")
              FeatureUtils.write(writer, feature, useProvidedFid = true)
              writer.close() must throwAn[IOException]
              tx.commit()
            }
            // verify we can read the existing features but the one we tried to write wasn't persisted
            WithClose(readOnlyDs.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
            }
            // verify data is being partitioned as expected
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              val typeInfo = TypeInfo(this.schema, sft)
              // initially everything is in the write ahead log
              foreach(Seq(typeInfo.tables.view, typeInfo.tables.writeAhead))(table => count(cx, table) mustEqual 10)
              foreach(Seq(typeInfo.tables.writeAheadPartitions, typeInfo.tables.mainPartitions))(table => count(cx, table) mustEqual 0)
              // manually invoke the scheduled crons so we don't have to wait
              WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
              WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
              // verify that data was sorted into the appropriate tables based on dtg
              count(cx, typeInfo.tables.view) mustEqual 10
              count(cx, typeInfo.tables.writeAhead) mustEqual 0
              count(cx, typeInfo.tables.writeAheadPartitions) must beGreaterThan(0)
              count(cx, typeInfo.tables.mainPartitions) must beGreaterThan(0)
            }

            // ensure we still get same results after running partitioning
            WithClose(readOnlyDs.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
            }
          }
        }
      }
    }
  }
}
