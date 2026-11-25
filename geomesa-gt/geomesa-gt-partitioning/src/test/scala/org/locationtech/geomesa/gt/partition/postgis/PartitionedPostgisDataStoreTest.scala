/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.dialect._
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose

class PartitionedPostgisDataStoreTest extends TestWithSharedPostgisContainer {

  import scala.collection.JavaConverters._

  "PartitionedPostgisDataStore" should {

    "work" in {
      WithClose(datastore()) { ds =>
        val sftNames = Seq("test", "test-abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz")
        foreach(sftNames) { name =>
          testSchema(ds, name) { sft =>
            val features = writeFeatures(ds, sft)

            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
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
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
            }

            features.foreach { feature =>
              WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter(s"IN('${feature.getID}')"), Transaction.AUTO_COMMIT)) { writer =>
                writer.hasNext must beTrue
                writer.next()
                writer.remove()
              }
            }

            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              reader.hasNext must beFalse
            }
          }
        }
      }
    }

    "default to using prepared statements" in {
      foreach(Seq(params, params + ("preparedStatements" -> "true"), params - "preparedStatements")) { params =>
        WithClose(datastore(params)) { ds =>
          ds.dialect must beAnInstanceOf[PartitionedPostgisPsDialect]
        }
      }
      WithClose(datastore(params + ("preparedStatements" -> "false"))) { ds =>
        ds.dialect must beAnInstanceOf[PartitionedPostgisDialect] // not partitioned
      }
    }

    "set appropriate user data for list and json attributes" in {
      WithClose(datastore(params)) { ds =>
        testSchema(ds, "attrtest") { sft =>
          ObjectType.selectType(sft.getDescriptor("name")) mustEqual Seq(ObjectType.LIST, ObjectType.STRING)
          ObjectType.selectType(sft.getDescriptor("props")) mustEqual Seq(ObjectType.STRING, ObjectType.JSON)
          ObjectType.selectType(sft.getDescriptor("dtg")) mustEqual Seq(ObjectType.DATE)
          ObjectType.selectType(sft.getDescriptor("geom")) mustEqual Seq(ObjectType.GEOMETRY, ObjectType.POINT)
        }
      }
    }

    "support idle_in_transaction_session_timeout" in {
      val params = this.params ++ Map("idle_in_transaction_session_timeout" -> "500ms", "fetch size" -> 1)
      WithClose(datastore(params)) { ds =>
        testSchema(ds, "timeout") { sft =>
          writeFeatures(ds, sft)
          // verify that statements will timeout and return an error
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            reader.hasNext must beTrue
            reader.next must not(beNull)
            Thread.sleep(600)
            reader.hasNext must throwAn[Exception]
          }
        }
      }
    }

    "return feature ids without the type name from feature writers" in {
      // note: the default geometry in the second spec isn't the first geometry, so features are re-typed by the store
      val specs = Seq("" -> spec, "-mapped" -> s"loc:Point:srid=4326,$spec")
      val tests = for { batch <- Seq("1", "3", "10"); (suffix, spec) <- specs } yield { (batch, suffix, spec) }
      foreach(tests) { case (batch, suffix, spec) =>
        WithClose(datastore(params ++ Map("Batch insert size" -> batch))) { ds =>
          testSchema(ds, s"writer-ids-$batch$suffix", spec) { sft =>
            val features = this.features(sft)
            // note: the jdbc writer sets the id on each feature once it has been inserted, which may be after
            // subsequent features are written, or when the writer is closed, depending on the batch size
            val (immediate, ids) = WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              features.map { feature =>
                val sf = FeatureUtils.write(writer, feature, useProvidedFid = true)
                // note: the writer re-uses features once they have been inserted, so we track the identifiers
                (sf.getID, sf.getIdentifier)
              }.unzip
            }
            immediate mustEqual features.map(_.getID)
            ids.map(_.getID) mustEqual features.map(_.getID)
            // existing features
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN('fid1')"), Transaction.AUTO_COMMIT)) { writer =>
              writer.hasNext must beTrue
              val sf = writer.next()
              sf.getID mustEqual "fid1"
              sf.setAttribute("age", Int.box(-1))
              writer.write()
              sf.getID mustEqual "fid1"
            }
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("age = -1")), Transaction.AUTO_COMMIT)) { reader =>
              CloseableIterator(reader).map(_.getID).toList mustEqual Seq("fid1")
            }
            // ids generated by the data store, which aren't set until the features have been inserted
            val generated = this.features(sft).map { feature =>
              feature.setAttribute("age", Int.box(100))
              feature.getUserData.remove(Hints.USE_PROVIDED_FID)
              feature
            }
            val added = ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, generated.asJava))
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("age = 100")), Transaction.AUTO_COMMIT)) { reader =>
              val ids = CloseableIterator(reader).map(_.getID).toList
              ids must haveLength(generated.length)
              added.asScala.map(_.getID) must containTheSameElementsAs(ids)
            }
            // ids generated by the data store through a feature writer
            val written = WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              generated.map { feature =>
                feature.setAttribute("age", Int.box(200))
                FeatureUtils.write(writer, feature).getIdentifier
              }
            }
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("age = 200")), Transaction.AUTO_COMMIT)) { reader =>
              val ids = CloseableIterator(reader).map(_.getID).toList
              ids must haveLength(generated.length)
              written.map(_.getID) must containTheSameElementsAs(ids)
            }
            // removing features through a feature writer
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("age = 200 OR IN('fid1')"), Transaction.AUTO_COMMIT)) { writer =>
              while (writer.hasNext) {
                writer.next()
                writer.remove()
              }
            }
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val ids = CloseableIterator(reader).map(_.getID).toList
              ids must haveLength(features.length - 1 + generated.length)
              ids must not(contain("fid1"))
              ids must not(containAnyOf(written.map(_.getID)))
            }
          }
        }
      }
    }

    "support attributes named 'fid'" in {
      WithClose(datastore(params)) { ds =>
        testSchema(ds, "fid_test", "fid:String,dtg:Date,*geom:Point:srid=4326") { sft =>
          sft.getAttributeDescriptors.get(0).getLocalName mustEqual "fid"

          val features = this.features(SimpleFeatureTypes.createType("", spec)).map { sf =>
            val retyped = ScalaSimpleFeature.create(sft, sf.getID)
            retyped.setAttribute("fid", sf.getAttribute("age"))
            retyped.setAttribute("dtg", sf.getAttribute("dtg"))
            retyped.setAttribute("geom", sf.getAttribute("geom"))
            retyped
          }

          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          val result = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
          result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))

          val byId =
            CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("IN ('fid0')")), Transaction.AUTO_COMMIT)).toList
          byId.map(compFromDb) mustEqual features.take(1).map(compWithFid(_, sft))

          val byAttribute =
            CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("fid = '0'")), Transaction.AUTO_COMMIT)).toList
          byAttribute.map(compFromDb) mustEqual features.take(1).map(compWithFid(_, sft))
        }
      }
    }
  }
}
