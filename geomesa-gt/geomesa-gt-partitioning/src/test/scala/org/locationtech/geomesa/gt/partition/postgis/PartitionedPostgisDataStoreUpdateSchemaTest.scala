/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.{SftUserData, VisCol}
import org.locationtech.geomesa.gt.partition.postgis.dialect._
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{MainView, UserDataTable}
import org.locationtech.geomesa.security.{AuthProviderParam, AuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Future, TimeUnit}
import scala.util.Try

class PartitionedPostgisDataStoreUpdateSchemaTest extends TestWithSharedPostgisContainer {

  import scala.collection.JavaConverters._

  // removes the 'age' attribute, and adds some new ones
  // note: doesn't include any user data, in order to verify that the existing configuration is retained
  val updatedSpec =
    "name:List[String],props:String:json=true,dtg:Date,*geom:Point:srid=4326," +
      "count:Long,line:LineString:srid=4326,tags:List[String],meta:String:json=true"

  def updatedFeatures(sft: SimpleFeatureType): Seq[SimpleFeature] = Seq.tabulate(10) { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("name", java.util.Collections.singletonList(s"updated$i"))
    builder.set("props", s"""{"age": ${i + 10}}""")
    builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
    builder.set("geom", WKTUtils.read(s"POINT(1 $i)"))
    builder.set("count", Long.box(i * 100L))
    builder.set("line", WKTUtils.read(s"LINESTRING(0 $i, 1 $i)"))
    builder.set("tags", java.util.List.of(s"tag$i", "tag"))
    builder.set("meta", s"""{"count": $i}""")
    val sf = builder.buildFeature(s"updated$i")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  def write(ds: DataStore, features: Seq[SimpleFeature]): Unit = {
    WithClose(new DefaultTransaction()) { tx =>
      WithClose(ds.getFeatureWriterAppend(features.head.getFeatureType.getTypeName, tx)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      tx.commit()
    }
  }

  def query(ds: DataStore, typeName: String, filter: String = "INCLUDE"): Seq[SimpleFeature] = {
    WithClose(ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)) { reader =>
      CloseableIterator(reader).toList.sortBy(_.getID)
    }
  }

  def runMaintenance(cx: Connection, generations: TypeInfo*): Unit = {
    generations.foreach { typeInfo =>
      WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
      WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
    }
  }

  // gets the type info for the current generation of a feature type
  def typeInfo(ds: PartitionedPostgisDataStore, typeName: String): TypeInfo = {
    val userData = WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
      UserDataTable.read(cx, this.schema, Seq(SftUserData.TypeName, SftUserData.PreviousView))
    }
    val superseded = userData.collect { case (_, SftUserData.PreviousView.key, view) => view }
    val view = userData.collectFirst {
      case (view, SftUserData.TypeName.key, name) if name == typeName && !superseded.contains(view) => view
    }
    TypeInfo(this.schema, SimpleFeatureTypes.renameSft(ds.getSchema(typeName), view.getOrElse(typeName)))
  }

  // gets the type names for a test, which may have '-renamed' appended to them
  def typeNames(ds: PartitionedPostgisDataStore, name: String): Seq[String] =
    ds.getTypeNames.toSeq.filter(n => n == name || n == s"$name-renamed")

  // user data, without things that are tied to the underlying tables
  def userData(sft: SimpleFeatureType): Seq[(AnyRef, AnyRef)] =
    sft.getUserData.asScala.toSeq.filterNot { case (k, _) => SftUserData.Values.exists(d => d.internal && d.key == k) }

  def exists(ds: PartitionedPostgisDataStore, table: TableIdentifier): Boolean = {
    WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
      WithClose(cx.createStatement()) { st =>
        WithClose(st.executeQuery(s"SELECT to_regclass('${table.qualified}') IS NOT NULL")) { rs =>
          rs.next() && rs.getBoolean(1)
        }
      }
    }
  }

  def attributes(sft: SimpleFeatureType): Seq[String] = sft.getAttributeDescriptors.asScala.map(_.getLocalName).toSeq

  def columns(cx: Connection, table: TableIdentifier): Seq[String] = {
    val sql =
      s"SELECT attname FROM pg_attribute WHERE attrelid = ${table.asRegclass} AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
    WithClose(cx.createStatement()) { st =>
      WithClose(st.executeQuery(sql)) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
      }
    }
  }

  "PartitionedPostgisDataStore" should {

    "add and remove attributes and rename an existing schema" in {
      val role = "update_schema_reader"
      WithClose(datastore()) { ds =>
        WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
          WithClose(cx.createStatement())(_.execute(s"CREATE ROLE $role;"))
        }
        def hasPrivilege(view: TableIdentifier): Boolean = {
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              WithClose(st.executeQuery(s"SELECT has_table_privilege('$role', ${view.asRegclass}, 'SELECT')")) { rs =>
                rs.next() && rs.getBoolean(1)
              }
            }
          }
        }
        // note: the second name is long enough to require an identifier alias
        foreach(Seq("update-schema", "update-schema-abcdefghijklmnopqrstuvwxyz_abcdefghijklm")) { name =>
          var generations = Seq.empty[TypeInfo]
          testSchema(ds, name) { sft =>
            val original = TypeInfo(this.schema, sft)
            val features = writeFeatures(ds, sft)
            val writeAhead = features.take(2).map { f =>
              val copy = SimpleFeatureBuilder.build(sft, f.getAttributes, s"wa${f.getID}")
              copy.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              copy
            }

            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              // move the data out of the write ahead log, so that we have data in each of our tables
              runMaintenance(cx, original)
              count(cx, original.tables.writeAheadPartitions) must beGreaterThan(0)
              count(cx, original.tables.mainPartitions) must beGreaterThan(0)
              // write some more features to the write ahead log
              write(ds, writeAhead)
              count(cx, original.tables.writeAhead) mustEqual 2
              // add a privilege to the view, to verify it's carried over to the new view
              WithClose(cx.createStatement())(_.execute(s"GRANT SELECT ON ${original.tables.view.name.qualified} TO $role;"))
            }

            ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))

            typeNames(ds, name) mustEqual Seq(name)
            val updated = ds.getSchema(sft.getTypeName)
            updated.getTypeName mustEqual sft.getTypeName
            attributes(updated) mustEqual Seq("name", "props", "dtg", "geom", "count", "line", "tags", "meta")
            updated.getGeometryDescriptor.getLocalName mustEqual "geom"
            // verify existing user data is retained
            updated.getUserData.asScala must containAllOf(userData(sft))

            // verify that a new data store picks up the changes
            WithClose(datastore()) { ds2 =>
              typeNames(ds2, name) mustEqual Seq(name)
              SimpleFeatureTypes.encodeType(ds2.getSchema(sft.getTypeName)) mustEqual SimpleFeatureTypes.encodeType(updated)
            }

            // the update creates a new generation, with new tables
            val current = typeInfo(ds, sft.getTypeName)
            generations = Seq(original, current)
            current.tables.view must not(beEqualTo(original.tables.view))
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              def tables(info: TypeInfo): Seq[TableConfig] =
                Seq(info.tables.view, info.tables.writeAhead, info.tables.writeAheadPartitions,
                  info.tables.mainPartitions, info.tables.spillPartitions)
              foreach(tables(current))(table => columns(cx, table.name) mustEqual Seq("fid") ++ attributes(updated))
              // the original tables aren't modified
              foreach(tables(original))(table => columns(cx, table.name) mustEqual Seq("fid") ++ attributes(sft))
            }
            hasPrivilege(current.tables.view.name) must beTrue
            // privileges are only carried over when the generation is created, so they can be revoked afterwards
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement())(_.execute(s"REVOKE SELECT ON ${current.tables.view.name.qualified} FROM $role;"))
            }
            ds.upgrade(ds.getSchema(sft.getTypeName))
            hasPrivilege(current.tables.view.name) must beFalse
            hasPrivilege(original.tables.view.name) must beTrue

            // existing features come back with nulls for the new attributes, and without the removed attribute
            val existing = features ++ writeAhead
            query(ds, sft.getTypeName, "count IS NULL").map(compFromDb) mustEqual existing.map(compWithFid(_, updated))

            // write some features with the new attributes
            val added = updatedFeatures(updated)
            write(ds, added)
            query(ds, sft.getTypeName, "count IS NOT NULL").map(compFromDb) mustEqual added.map(compWithFid(_, updated))
            query(ds, sft.getTypeName, "count = 300").map(_.getID) mustEqual Seq("updated3")
            query(ds, sft.getTypeName, "IN('updated3', 'fid2')").map(_.getID) mustEqual Seq("fid2", "updated3")

            // writes from a client that is still using the original view go to the original generation, so that the
            // client can read them back, but are still exposed through the current view
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement()) { st =>
                st.executeUpdate(
                  s"""INSERT INTO ${original.tables.view.name.qualified} (fid, age, dtg, geom)
                     |  VALUES ('stale', 99, now()::timestamp, ST_GeomFromText('POINT(5 5)', 4326))""".stripMargin)
              }
              count(cx, original.tables.writeAhead) mustEqual 3
              count(cx, current.tables.writeAhead) mustEqual 10
              WithClose(cx.createStatement()) { st =>
                WithClose(st.executeQuery(s"SELECT count(*) FROM ${original.tables.view.name.qualified} WHERE fid = 'stale'")) { rs =>
                  rs.next() must beTrue
                  rs.getInt(1) mustEqual 1
                }
              }
            }
            query(ds, sft.getTypeName, "IN('stale')") must haveLength(1)
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN('stale')"), Transaction.AUTO_COMMIT)) { writer =>
              writer.hasNext must beTrue
              writer.next()
              writer.remove()
            }

            // update an existing feature that is in a partition table for the original generation
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN('fid9')"), Transaction.AUTO_COMMIT)) { writer =>
              writer.hasNext must beTrue
              val sf = writer.next()
              sf.getID mustEqual "fid9"
              sf.setAttribute("count", Long.box(-1L))
              sf.setAttribute("tags", java.util.Collections.singletonList("modified"))
              writer.write()
            }
            val modified = query(ds, sft.getTypeName, "count = -1")
            modified.map(_.getID) mustEqual Seq("fid9")
            modified.head.getAttribute("tags") mustEqual java.util.Collections.singletonList("modified")
            modified.head.getAttribute("name") mustEqual features.last.getAttribute("name")
            modified.head.getAttribute("geom") mustEqual features.last.getAttribute("geom")

            // verify that partition maintenance still works for both generations
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              runMaintenance(cx, generations: _*)
              count(cx, original.tables.writeAhead) mustEqual 0
              count(cx, current.tables.writeAhead) mustEqual 0
              count(cx, current.tables.view) mustEqual 22
            }
            query(ds, sft.getTypeName, "count >= 0").map(compFromDb) mustEqual added.map(compWithFid(_, updated))
            query(ds, sft.getTypeName, "count IS NULL").map(compFromDb) mustEqual
                existing.filter(_.getID != "fid9").map(compWithFid(_, updated))

            // deletes, from both generations
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("count >= 0 OR IN('fid0')"), Transaction.AUTO_COMMIT)) { writer =>
              while (writer.hasNext) {
                writer.next()
                writer.remove()
              }
            }
            query(ds, sft.getTypeName) must haveLength(11)

            // rename the schema
            val renamed = s"${sft.getTypeName}-renamed"
            ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(updated, renamed))
            try {
              typeNames(ds, name) mustEqual Seq(renamed)
              ds.getSchema(sft.getTypeName) must throwAn[java.io.IOException]
              // the old name can't be used to remove the views that still back the renamed schema
              ds.removeSchema(sft.getTypeName) must throwAn[IllegalArgumentException]
              ds.removeSchema(current.tables.view.name.raw) must throwAn[IllegalArgumentException]
              foreach(generations)(info => exists(ds, info.tables.view.name) must beTrue)
              foreach(Seq(ds.getSchema(renamed), WithClose(datastore())(_.getSchema(renamed)))) { result =>
                result.getTypeName mustEqual renamed
                attributes(result) mustEqual attributes(updated)
                result.getUserData.asScala must containAllOf(userData(sft))
              }
              // the underlying view and tables aren't renamed
              typeInfo(ds, renamed).tables mustEqual current.tables
              query(ds, renamed) must haveLength(11)
              write(ds, updatedFeatures(ds.getSchema(renamed)))
              query(ds, renamed, "count >= 0").map(compFromDb) mustEqual
                  updatedFeatures(updated).map(compWithFid(_, ds.getSchema(renamed)))
              WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
                count(cx, current.tables.writeAhead) mustEqual 10
                runMaintenance(cx, generations: _*)
                count(cx, current.tables.writeAhead) mustEqual 0
                count(cx, current.tables.view) mustEqual 21
              }

              // verify we can re-use the old name, without interfering with the renamed schema
              ds.createSchema(SimpleFeatureTypes.createType(sft.getTypeName, spec))
              try {
                typeNames(ds, name) must containTheSameElementsAs(Seq(sft.getTypeName, renamed))
                val reused = ds.getSchema(sft.getTypeName)
                reused.getTypeName mustEqual sft.getTypeName
                attributes(reused) mustEqual attributes(sft)
                val reusedInfo = typeInfo(ds, sft.getTypeName)
                reusedInfo.tables.view must not(beEqualTo(original.tables.view))
                reusedInfo.tables.writeAhead must not(beEqualTo(original.tables.writeAhead))
                query(ds, sft.getTypeName) must beEmpty
                writeFeatures(ds, reused)
                query(ds, sft.getTypeName).map(compFromDb) mustEqual features.map(compWithFid(_, reused))
                query(ds, renamed) must haveLength(21)
                // can't rename over an existing schema, or create a schema that already exists
                ds.updateSchema(renamed, SimpleFeatureTypes.renameSft(ds.getSchema(renamed), sft.getTypeName)) must
                    throwAn[IllegalArgumentException]
                ds.createSchema(SimpleFeatureTypes.createType(renamed, spec)) must throwAn[IllegalArgumentException]
                ds.createSchema(SimpleFeatureTypes.createType(sft.getTypeName, spec)) must throwAn[IllegalArgumentException]
              } finally {
                ds.removeSchema(sft.getTypeName)
              }
              typeNames(ds, name) mustEqual Seq(renamed)
              query(ds, renamed) must haveLength(21)
            } finally {
              // rename back to the original name so that it gets cleaned up
              ds.updateSchema(renamed, SimpleFeatureTypes.renameSft(ds.getSchema(renamed), sft.getTypeName))
            }
            typeNames(ds, name) mustEqual Seq(sft.getTypeName)
            query(ds, sft.getTypeName) must haveLength(21)
          }
          // removing the schema removes all the generations
          generations must haveLength(2)
          foreach(generations) { info =>
            foreach(Seq(info.tables.view, info.tables.writeAhead, info.tables.mainPartitions)) { table =>
              exists(ds, table.name) must beFalse
            }
          }
        }
      }
    }

    "update schemas without waiting for locks on the existing view or tables" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-locks") { sft =>
          val original = TypeInfo(this.schema, sft)
          val features = writeFeatures(ds, sft)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            cx.setAutoCommit(false)
            var future: Future[_] = null
            try {
              WithClose(cx.createStatement()) { st =>
                // simulates a long-running query against the view - note that this also locks all the underlying tables
                st.execute(s"LOCK TABLE ${original.tables.view.name.qualified} IN ACCESS SHARE MODE")
                // simulates long-running partition maintenance
                Seq(original.tables.writeAheadPartitions, original.tables.mainPartitions, original.tables.spillPartitions).foreach { t =>
                  st.execute(s"LOCK TABLE ONLY ${t.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE")
                }
              }
              val count = new AtomicInteger(-1)
              future = CachedThreadPool.submit(() => {
                WithClose(datastore()) { ds2 =>
                  ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
                  // reads and writes aren't blocked either
                  write(ds2, updatedFeatures(ds2.getSchema(sft.getTypeName)))
                  count.set(query(ds2, sft.getTypeName).length)
                }
              })
              future.get(60, TimeUnit.SECONDS)
              count.get mustEqual 20
            } finally {
              cx.rollback()
              cx.setAutoCommit(true)
              if (future != null) {
                Try(future.get(60, TimeUnit.SECONDS))
              }
            }
          }
          // note: our existing data store will still have the original feature type cached
          attributes(ds.getSchema(sft.getTypeName)) mustEqual attributes(sft)
          query(ds, sft.getTypeName).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
          WithClose(datastore()) { ds2 =>
            val updated = ds2.getSchema(sft.getTypeName)
            attributes(updated) mustEqual attributes(SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
            query(ds2, sft.getTypeName, "count IS NULL").map(compFromDb) mustEqual features.map(compWithFid(_, updated))
          }
        }
      }
    }

    "retire previous generations once their data has aged off" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-retire") { sft =>
          val original = TypeInfo(this.schema, sft)
          writeFeatures(ds, sft)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(runMaintenance(_, original))

          // create two additional generations
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
          val second = typeInfo(ds, sft.getTypeName)
          write(ds, updatedFeatures(ds.getSchema(sft.getTypeName)))
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, s"$updatedSpec,extra:String"))
          val third = typeInfo(ds, sft.getTypeName)
          val updated = ds.getSchema(sft.getTypeName)
          Seq(original, second, third).map(_.tables.view.name.raw).distinct must haveLength(3)
          query(ds, sft.getTypeName) must haveLength(20)

          def dataTables(info: TypeInfo): Seq[TableConfig] =
            Seq(info.tables.writeAhead, info.tables.writeAheadPartitions, info.tables.mainPartitions, info.tables.spillPartitions)
          def previous(): Map[String, String] = {
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              UserDataTable.read(cx, this.schema, Seq(SftUserData.PreviousView)).map { case (view, _, prev) => view -> prev }.toMap
            }
          }
          previous() must containAllOf(Seq(
            third.tables.view.name.raw -> second.tables.view.name.raw,
            second.tables.view.name.raw -> original.tables.view.name.raw))

          def retireProcedure(info: TypeInfo): Boolean = {
            val sql = s"SELECT EXISTS (SELECT FROM pg_proc WHERE proname = ${RetirePreviousGeneration.name(info).asLiteral})"
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement())(st => WithClose(st.executeQuery(sql))(rs => rs.next() && rs.getBoolean(1)))
            }
          }
          Seq(original, second, third).map(retireProcedure) mustEqual Seq(false, true, true)

          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            // nothing is retired while there is still data
            runMaintenance(cx, original, second, third)
            foreach(Seq(original, second, third))(info => exists(ds, info.tables.view.name) must beTrue)

            // age-off that is configured directly in the user data table is applied to the previous generations
            def maxPartitions(): Seq[Option[String]] =
              Seq(original, second, third).map(info => UserDataTable.read(cx, this.schema, info.typeName).get(SftUserData.MaxPartitions.key))
            maxPartitions() mustEqual Seq(None, None, None)
            WithClose(cx.createStatement()) { st =>
              st.executeUpdate(
                s"""INSERT INTO "${this.schema}".${UserDataTable.Name.quoted} (type_name, key, value, mutable)
                   |  VALUES (${literal(third.typeName)}, ${literal(SftUserData.MaxPartitions.key)}, '1000', true)""".stripMargin)
            }
            // note: each generation updates the one before it
            runMaintenance(cx, third, second)
            maxPartitions() mustEqual Seq(Some("1000"), Some("1000"), Some("1000"))
            WithClose(cx.createStatement()) { st =>
              st.executeUpdate(
                s"""UPDATE "${this.schema}".${UserDataTable.Name.quoted} SET value = '2000'
                   |  WHERE type_name = ${literal(third.typeName)} AND key = ${literal(SftUserData.MaxPartitions.key)}""".stripMargin)
            }
            runMaintenance(cx, third, second)
            maxPartitions() mustEqual Seq(Some("2000"), Some("2000"), Some("2000"))
            WithClose(cx.createStatement()) { st =>
              st.executeUpdate(
                s"""DELETE FROM "${this.schema}".${UserDataTable.Name.quoted}
                   |  WHERE type_name = ${literal(third.typeName)} AND key = ${literal(SftUserData.MaxPartitions.key)}""".stripMargin)
            }
            runMaintenance(cx, third, second)
            maxPartitions() mustEqual Seq(None, None, None)
            foreach(Seq(original, second, third))(info => exists(ds, info.tables.view.name) must beTrue)
            query(ds, sft.getTypeName) must haveLength(20)

            // simulate the second generation aging off - it can't be retired until the first generation is
            WithClose(cx.createStatement()) { st =>
              dataTables(second).foreach(t => st.execute(s"DELETE FROM ${t.name.qualified}"))
            }
            runMaintenance(cx, original, second, third)
            foreach(Seq(original, second, third))(info => exists(ds, info.tables.view.name) must beTrue)
            query(ds, sft.getTypeName) must haveLength(10)

            // simulate the first generation aging off
            WithClose(cx.createStatement()) { st =>
              dataTables(original).foreach(t => st.execute(s"DELETE FROM ${t.name.qualified}"))
            }
            // retirement is skipped if it can't get a lock on the view
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx2 =>
              cx2.setAutoCommit(false)
              try {
                WithClose(cx2.createStatement())(_.execute(s"LOCK TABLE ${second.tables.view.name.qualified} IN ACCESS SHARE MODE"))
                runMaintenance(cx, second)
                exists(ds, original.tables.view.name) must beTrue
              } finally {
                cx2.rollback()
                cx2.setAutoCommit(true)
              }
            }
            // retirement waits for an uncommitted write to the previous generation, and the previous generation
            // isn't dropped if the write is committed while it's waiting
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx2 =>
              cx2.setAutoCommit(false)
              var future: Future[_] = null
              try {
                WithClose(cx2.createStatement()) { st =>
                  st.executeUpdate(
                    s"""INSERT INTO ${original.tables.view.name.qualified} (fid, age, dtg, geom)
                       |  VALUES ('uncommitted', 99, now()::timestamp, ST_GeomFromText('POINT(5 5)', 4326))""".stripMargin)
                }
                future = CachedThreadPool.submit(() => WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(runMaintenance(_, second)))
                // wait for the retirement to block on our write
                val waiting = s"SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = ${original.tables.writeAhead.name.asRegclass}"
                def isWaiting: Boolean =
                  WithClose(cx.createStatement())(st => WithClose(st.executeQuery(waiting))(rs => rs.next() && rs.getInt(1) > 0))
                val timeout = System.currentTimeMillis() + 30000
                var blocked = false
                while (!blocked && !future.isDone && System.currentTimeMillis() < timeout) {
                  blocked = isWaiting
                  if (!blocked) {
                    Thread.sleep(10)
                  }
                }
                blocked must beTrue
                cx2.commit()
                future.get(60, TimeUnit.SECONDS)
              } finally {
                cx2.rollback()
                cx2.setAutoCommit(true)
                if (future != null) {
                  Try(future.get(60, TimeUnit.SECONDS))
                }
              }
            }
            exists(ds, original.tables.view.name) must beTrue
            query(ds, sft.getTypeName, "IN('uncommitted')") must haveLength(1)
            // retirement is still skipped now that the previous generation has data again
            runMaintenance(cx, second)
            exists(ds, original.tables.view.name) must beTrue
            query(ds, sft.getTypeName, "IN('uncommitted')") must haveLength(1)
            WithClose(cx.createStatement()) { st =>
              dataTables(original).foreach(t => st.execute(s"DELETE FROM ${t.name.qualified}"))
            }
            // an error checking the previous generation doesn't fail maintenance
            val spill = original.tables.spillPartitions.name
            val moved = TableIdentifier(this.schema, s"${spill.raw}_moved")
            WithClose(cx.createStatement())(_.execute(s"ALTER TABLE ${spill.qualified} RENAME TO ${moved.quoted}"))
            try {
              runMaintenance(cx, second)
              exists(ds, original.tables.view.name) must beTrue
              previous().get(second.tables.view.name.raw) must beSome(original.tables.view.name.raw)
              retireProcedure(second) must beTrue
            } finally {
              WithClose(cx.createStatement())(_.execute(s"ALTER TABLE ${moved.qualified} RENAME TO ${spill.quoted}"))
            }
            // a view that depends on the previous generation prevents it from being retired, without failing maintenance
            val dependent = TableIdentifier(this.schema, s"${sft.getTypeName}-dependent")
            WithClose(cx.createStatement()) { st =>
              st.execute(s"CREATE VIEW ${dependent.qualified} AS SELECT * FROM ${original.tables.view.name.qualified}")
            }
            try {
              runMaintenance(cx, second)
              foreach(dataTables(original) ++ Seq(original.tables.view))(t => exists(ds, t.name) must beTrue)
              previous().get(second.tables.view.name.raw) must beSome(original.tables.view.name.raw)
              retireProcedure(second) must beTrue
            } finally {
              WithClose(cx.createStatement())(_.execute(s"DROP VIEW ${dependent.qualified}"))
            }
            // the second generation retires the first...
            runMaintenance(cx, second)
            foreach(dataTables(original) ++ Seq(original.tables.view))(t => exists(ds, t.name) must beFalse)
            exists(ds, second.tables.view.name) must beTrue
            previous().get(second.tables.view.name.raw) must beNone
            // the procedure is dropped once it has retired the previous generation
            Seq(second, third).map(retireProcedure) mustEqual Seq(false, true)
            // ... and then the third generation retires the second
            runMaintenance(cx, third)
            foreach(dataTables(second) ++ Seq(second.tables.view))(t => exists(ds, t.name) must beFalse)
            previous().get(third.tables.view.name.raw) must beNone
            UserDataTable.read(cx, this.schema, second.typeName) must beEmpty
            UserDataTable.read(cx, this.schema, original.typeName) must beEmpty
            retireProcedure(third) must beFalse
            // maintenance keeps working after the previous generations are gone
            runMaintenance(cx, third)
          }

          // verify things still work
          typeNames(ds, sft.getTypeName) mustEqual Seq(sft.getTypeName)
          query(ds, sft.getTypeName) must beEmpty
          val features = updatedFeatures(updated)
          write(ds, features)
          WithClose(datastore()) { ds2 =>
            query(ds2, sft.getTypeName).map(compFromDb) mustEqual features.map(compWithFid(_, updated))
          }
          WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN('updated0')"), Transaction.AUTO_COMMIT)) { writer =>
            writer.hasNext must beTrue
            writer.next().setAttribute("extra", "modified")
            writer.write()
          }
          query(ds, sft.getTypeName, "extra = 'modified'").map(_.getID) mustEqual Seq("updated0")
          // updates and deletes of features that don't exist
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              val view = third.tables.view.name.qualified
              st.executeUpdate(s"UPDATE $view SET extra = 'foo' WHERE fid = 'missing'") mustEqual 0
              st.executeUpdate(s"DELETE FROM $view WHERE fid = 'missing'") mustEqual 0
            }
          }
          ds.upgrade(ds.getSchema(sft.getTypeName))
          query(ds, sft.getTypeName) must haveLength(10)
          // the retired generation isn't restored from the cached feature type
          previous().get(third.tables.view.name.raw) must beNone

          // a new feature type that re-uses the name of a retired view is not retired
          // note: only renaming the feature type doesn't re-create any of the procedures
          val renamed = s"${sft.getTypeName}-renamed"
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(ds.getSchema(sft.getTypeName), renamed))
          try {
            // note: the retired view was dropped outside of the data store, but can still be re-used
            ds.createSchema(SimpleFeatureTypes.createType(sft.getTypeName, spec))
            exists(ds, original.tables.view.name) must beTrue
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              runMaintenance(cx, third, original)
              UserDataTable.read(cx, this.schema, original.typeName) must not(beEmpty)
            }
            foreach(dataTables(original) ++ Seq(original.tables.view))(t => exists(ds, t.name) must beTrue)
            ds.getTypeNames.toSeq.filter(_.startsWith(sft.getTypeName)) must containTheSameElementsAs(Seq(sft.getTypeName, renamed))
            writeFeatures(ds, ds.getSchema(sft.getTypeName))
            query(ds, sft.getTypeName) must haveLength(10)
            query(ds, renamed) must haveLength(10)
          } finally {
            ds.removeSchema(renamed)
          }
        }
      }
    }

    "create schemas concurrently when the type name is being used by another view" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "create-schema-concurrent") { sft =>
          // rename the feature type, so that its view is still using the original name
          val renamed = s"${sft.getTypeName}-renamed"
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(sft, renamed))
          try {
            val stores = Seq.fill(4)(datastore())
            try {
              val results = new java.util.concurrent.ConcurrentLinkedQueue[Try[Unit]]()
              val creates = stores.map { store =>
                CachedThreadPool.submit(() => {
                  results.add(Try(store.createSchema(SimpleFeatureTypes.createType(sft.getTypeName, spec))))
                  ()
                })
              }
              creates.foreach(_.get(1, TimeUnit.MINUTES))
              results.asScala.toSeq must haveLength(stores.length)
              results.asScala.count(_.isSuccess) mustEqual 1
              foreach(results.asScala.toSeq.filter(_.isFailure))(_.failed.get must beAnInstanceOf[IllegalArgumentException])
            } finally {
              stores.foreach(_.dispose())
            }
            foreach(Seq(ds, datastore())) { store =>
              try {
                store.getTypeNames.toSeq.filter(_.startsWith(sft.getTypeName)) must
                    containTheSameElementsAs(Seq(sft.getTypeName, renamed))
              } finally {
                if (store ne ds) { store.dispose() }
              }
            }
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              val names = UserDataTable.read(cx, this.schema, Seq(SftUserData.TypeName))
              names.count { case (_, _, name) => name == sft.getTypeName } mustEqual 1
            }
          } finally {
            ds.removeSchema(renamed)
          }
        }
      }
    }

    "update schemas with visibilities" in {
      val provider = new AuthorizationsProvider {
        override def getAuthorizations: java.util.List[String] = java.util.Collections.singletonList("user")
        override def configure(params: java.util.Map[String, _]): Unit = {}
      }
      WithClose(datastore(params ++ Map(AuthProviderParam.key -> provider))) { ds =>
        val sft = this.sft("update-schema-vis", s"$spec,pg.vis.enabled=true")
        val renamed = this.sft("update-schema-vis-renamed", updatedSpec)
        ds.createSchema(sft)
        try {
          val original = TypeInfo(this.schema, ds.getSchema(sft.getTypeName))
          // note: every other feature is not visible to our user
          def withVis(features: Seq[SimpleFeature]): Seq[SimpleFeature] =
            features.zipWithIndex.map { case (f, i) => SecurityUtils.setFeatureVisibility(f, if (i % 2 == 0) { "user" } else { "admin" }) }
          def visible(features: Seq[SimpleFeature]): Seq[SimpleFeature] =
            features.zipWithIndex.collect { case (f, i) if i % 2 == 0 => f }

          val features = withVis(this.features(ds.getSchema(sft.getTypeName)))
          write(ds, features)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(runMaintenance(_, original))

          // add and remove columns, and rename, all at once
          ds.updateSchema(sft.getTypeName, renamed)

          typeNames(ds, sft.getTypeName) mustEqual Seq(renamed.getTypeName)
          val updated = ds.getSchema(renamed.getTypeName)
          updated.indexOf(VisCol) mustEqual -1
          attributes(updated) mustEqual attributes(renamed)
          SftUserData.VisEnabled.get(updated) must beTrue
          val current = typeInfo(ds, renamed.getTypeName)

          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            columns(cx, current.tables.view.name) mustEqual Seq("fid") ++ attributes(updated) ++ Seq(VisCol)
          }

          val result = query(ds, updated.getTypeName)
          result.map(compFromDb) mustEqual visible(features).map(compWithFid(_, updated))
          result.map(SecurityUtils.getVisibility) must contain(be_==("user")).foreach

          // write through both the feature writer and the feature store
          val added = withVis(updatedFeatures(updated))
          write(ds, added.take(5))
          ds.getFeatureSource(updated.getTypeName).addFeatures(new ListFeatureCollection(updated, added.drop(5).asJava)).asScala.map(_.getID) mustEqual
              added.drop(5).map(_.getID)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            count(cx, current.tables.writeAhead) mustEqual 10
            runMaintenance(cx, original, current)
            count(cx, current.tables.writeAhead) mustEqual 0
          }
          foreach(Seq(null, Array("count", "dtg", "geom"), Array("meta"), Array("name", "tags"))) { transforms =>
            val query = new Query(updated.getTypeName, ECQL.toFilter("count IS NOT NULL"), transforms: _*)
            val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
            results.map(compFromDb) mustEqual visible(added).map(compWithFid(_, updated, Option(transforms)))
            results.map(SecurityUtils.getVisibility) must contain(be_==("user")).foreach
            val fromFeatureSource =
              CloseableIterator(ds.getFeatureSource(updated.getTypeName).getFeatures(query).features()).toList.sortBy(_.getID)
            fromFeatureSource mustEqual results
          }
          ds.getFeatureSource(updated.getTypeName).getFeatures.size() mustEqual 10

          // update features in both generations
          foreach(Seq("updated0", "fid0")) { fid =>
            WithClose(ds.getFeatureWriter(updated.getTypeName, ECQL.toFilter(s"IN('$fid')"), Transaction.AUTO_COMMIT)) { writer =>
              writer.hasNext must beTrue
              val sf = writer.next()
              sf.setAttribute("count", Long.box(-1L))
              writer.write()
            }
            ok
          }
          val modified = query(ds, updated.getTypeName, "count = -1")
          modified.map(_.getID) mustEqual Seq("fid0", "updated0")
          modified.last.getAttribute("meta") mustEqual added.head.getAttribute("meta")
          modified.last.getAttribute("tags") mustEqual added.head.getAttribute("tags")
          modified.head.getAttribute("name") mustEqual features.head.getAttribute("name")
          modified.map(SecurityUtils.getVisibility) mustEqual Seq("user", "user")

          // update an attribute that is modified in place
          val dtg = added(2).getAttribute("dtg").asInstanceOf[java.util.Date].getTime - 1000
          WithClose(ds.getFeatureWriter(updated.getTypeName, ECQL.toFilter("IN('updated2')"), Transaction.AUTO_COMMIT)) { writer =>
            writer.hasNext must beTrue
            val sf = writer.next()
            val date = sf.getAttribute("dtg").asInstanceOf[java.util.Date]
            date.setTime(dtg)
            sf.setAttribute("dtg", date)
            writer.write()
          }
          query(ds, updated.getTypeName, "IN('updated2')").map(_.getAttribute("dtg").asInstanceOf[java.util.Date].getTime) mustEqual
              Seq(dtg)

          // feature ids that start with the name of the view are returned as-is
          val prefixed = s"${current.tables.view.name.raw}.updated4"
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            // note: the feature has been moved out of the write ahead table by partition maintenance
            val updates = Seq(current.tables.writeAheadPartitions, current.tables.mainPartitions).map { table =>
              WithClose(cx.prepareStatement(s"UPDATE ${table.name.qualified} SET fid = ? WHERE fid = 'updated4'")) { st =>
                st.setString(1, prefixed)
                st.executeUpdate()
              }
            }
            updates.sum mustEqual 1
          }
          query(ds, updated.getTypeName).map(_.getID) must contain(prefixed)
          CloseableIterator(ds.getFeatureSource(updated.getTypeName).getFeatures.features()).map(_.getID).toList must contain(prefixed)
          WithClose(ds.getFeatureWriter(updated.getTypeName, ECQL.toFilter("count = 400"), Transaction.AUTO_COMMIT)) { writer =>
            writer.hasNext must beTrue
            writer.next().getID mustEqual prefixed
          }

          // the vis column name can't be used
          ds.updateSchema(updated.getTypeName, SimpleFeatureTypes.createType(updated.getTypeName, s"$updatedSpec,$VisCol:String")) must
              throwAn[IllegalArgumentException]
          // can't disable vis
          val disabled = SimpleFeatureTypes.copy(updated)
          disabled.getUserData.put(SftUserData.VisEnabled.key, "false")
          ds.updateSchema(updated.getTypeName, disabled) must throwAn[UnsupportedOperationException]
        } finally {
          ds.getTypeNames.filter(Seq(sft.getTypeName, renamed.getTypeName).contains).foreach(ds.removeSchema)
        }
      }
    }

    "keep the existing schema working if an update fails" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-failure") { sft =>
          val original = TypeInfo(this.schema, sft)
          val features = writeFeatures(ds, sft)
          // everything in the database that is associated with the feature type, including any new generations
          def objects(): Seq[String] = {
            val prefix = s"'${sft.getTypeName}%'"
            val sql =
              s"""SELECT 'relation: ' || relname FROM pg_class WHERE relname LIKE $prefix
                 |UNION ALL SELECT 'routine: ' || proname FROM pg_proc WHERE proname LIKE $prefix
                 |UNION ALL SELECT 'job: ' || jobname FROM cron.job WHERE jobname LIKE $prefix
                 |UNION ALL SELECT 'user data: ' || type_name || ' ' || key || '=' || value
                 |  FROM ${TableIdentifier(this.schema, UserDataTable.Name.raw).qualified} WHERE type_name LIKE $prefix
                 |""".stripMargin
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement()) { st =>
                WithClose(st.executeQuery(sql)) { rs =>
                  Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList.sorted
                }
              }
            }
          }
          val before = objects()
          before must not(beEmpty)
          // the grant for a role that doesn't exist will fail, after everything for the new generation has been created
          WithClose(datastore(params ++ Map(PartitionedPostgisDataStoreParams.ReadAccessRoles.key -> "update_schema_missing"))) { ds2 =>
            ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec)) must throwAn[Exception]
          }
          // the new generation is created atomically, so nothing is left behind
          objects() mustEqual before
          // a tablespace that doesn't exist will fail before the main view for the new generation has been created
          val tablespace = SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec)
          tablespace.getUserData.put(SftUserData.MainTableSpace.key, "update_schema_missing")
          ds.updateSchema(sft.getTypeName, tablespace) must throwAn[Exception]
          objects() mustEqual before
          // the partially created generation is cleaned up
          typeNames(ds, sft.getTypeName) mustEqual Seq(sft.getTypeName)
          foreach(Seq(ds.getSchema(sft.getTypeName), WithClose(datastore())(_.getSchema(sft.getTypeName)))) { result =>
            attributes(result) mustEqual attributes(sft)
          }
          typeInfo(ds, sft.getTypeName).tables.view.name mustEqual original.tables.view.name
          // inserts into the existing view still go to the existing tables
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              st.executeUpdate(
                s"""INSERT INTO ${original.tables.view.name.qualified} (fid, age, dtg, geom)
                   |  VALUES ('after', 99, now()::timestamp, ST_GeomFromText('POINT(5 5)', 4326))""".stripMargin)
            }
            count(cx, original.tables.writeAhead) mustEqual 11
          }
          query(ds, sft.getTypeName).map(_.getID) mustEqual Seq("after") ++ features.map(_.getID)
        }
      }
    }

    "rename and re-configure a schema in a single update, or do neither if it fails" in {
      WithClose(datastore()) { ds =>
        val sft = this.sft("update-schema-rename-config")
        val renamed = this.sft("update-schema-rename-config-renamed")
        ds.createSchema(sft)
        try {
          val features = writeFeatures(ds, sft)
          def update(ds: DataStore, maxPartitions: Int): Unit = {
            val updated = SimpleFeatureTypes.renameSft(ds.getSchema(sft.getTypeName), renamed.getTypeName)
            updated.getUserData.put(SftUserData.MaxPartitions.key, String.valueOf(maxPartitions))
            ds.updateSchema(sft.getTypeName, updated)
          }
          // the grant for a role that doesn't exist will fail, after the configuration has been re-created
          WithClose(datastore(params ++ Map(PartitionedPostgisDataStoreParams.ReadAccessRoles.key -> "update_schema_missing"))) { ds2 =>
            update(ds2, 10) must throwAn[Exception]
          }
          foreach(Seq(ds, datastore())) { check =>
            try {
              typeNames(check, sft.getTypeName) mustEqual Seq(sft.getTypeName)
              SftUserData.MaxPartitions.get(check.getSchema(sft.getTypeName)) must beNone
            } finally {
              if (check ne ds) { check.dispose() }
            }
          }
          update(ds, 10)
          foreach(Seq(ds, datastore())) { check =>
            try {
              typeNames(check, sft.getTypeName) mustEqual Seq(renamed.getTypeName)
              SftUserData.MaxPartitions.get(check.getSchema(renamed.getTypeName)) must beSome(10)
              query(check, renamed.getTypeName).map(_.getID) mustEqual features.map(_.getID)
            } finally {
              if (check ne ds) { check.dispose() }
            }
          }
        } finally {
          ds.getTypeNames.filter(Seq(sft.getTypeName, renamed.getTypeName).contains).foreach(ds.removeSchema)
        }
      }
    }

    "remove all the generations of a schema, or none of them if it fails" in {
      WithClose(datastore()) { ds =>
        val sft = this.sft("update-schema-remove")
        val dependent = TableIdentifier(this.schema, "update_schema_remove_dependent")
        def execute(sql: String): Unit =
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(cx => WithClose(cx.createStatement())(_.execute(sql)))
        def tables(info: TypeInfo): Seq[TableIdentifier] = {
          val tables = Seq(info.tables.view, info.tables.writeAhead, info.tables.writeAheadPartitions,
            info.tables.mainPartitions, info.tables.spillPartitions, info.tables.analyzeQueue, info.tables.sortQueue)
          tables.map(_.name)
        }
        def scheduled(info: TypeInfo): Boolean = {
          val sql = s"SELECT EXISTS (SELECT FROM cron.job WHERE jobname = ${PartitionMaintenance.jobName(info).quoted})"
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement())(st => WithClose(st.executeQuery(sql))(rs => rs.next() && rs.getBoolean(1)))
          }
        }
        ds.createSchema(sft)
        try {
          val original = TypeInfo(this.schema, ds.getSchema(sft.getTypeName))
          val features = writeFeatures(ds, ds.getSchema(sft.getTypeName))
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
          val second = typeInfo(ds, sft.getTypeName)
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, s"$updatedSpec,extra:String"))
          val current = typeInfo(ds, sft.getTypeName)
          Seq(original, second, current).map(_.tables.view.name.raw).distinct must haveLength(3)

          // the original generation can't be removed while something else depends on its view
          execute(s"CREATE VIEW ${dependent.qualified} AS SELECT fid FROM ${original.tables.view.name.qualified}")
          ds.removeSchema(sft.getTypeName) must throwAn[Exception]
          // nothing is removed, and the schema keeps working
          foreach(Seq(original, second, current)) { info =>
            foreach(tables(info))(exists(ds, _) must beTrue)
            scheduled(info) must beTrue
          }
          typeNames(ds, sft.getTypeName) mustEqual Seq(sft.getTypeName)
          WithClose(datastore())(typeNames(_, sft.getTypeName) mustEqual Seq(sft.getTypeName))
          query(ds, sft.getTypeName).map(_.getID) must containTheSameElementsAs(features.map(_.getID))
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(runMaintenance(_, current, second, original))

          // the schema can be removed once the dependent view is gone
          execute(s"DROP VIEW ${dependent.qualified}")
          ds.removeSchema(sft.getTypeName)
          foreach(Seq(original, second, current)) { info =>
            foreach(tables(info))(exists(ds, _) must beFalse)
            scheduled(info) must beFalse
          }
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            UserDataTable.read(cx, this.schema, current.typeName) must beEmpty
            UserDataTable.read(cx, this.schema, second.typeName) must beEmpty
            UserDataTable.read(cx, this.schema, original.typeName) must beEmpty
          }
          typeNames(ds, sft.getTypeName) must beEmpty

          // the schema can be re-created
          ds.createSchema(sft)
          typeNames(ds, sft.getTypeName) mustEqual Seq(sft.getTypeName)
          val recreated = writeFeatures(ds, ds.getSchema(sft.getTypeName))
          query(ds, sft.getTypeName).map(_.getID) mustEqual recreated.map(_.getID)
        } finally {
          execute(s"DROP VIEW IF EXISTS ${dependent.qualified}")
          ds.getTypeNames.filter(_ == sft.getTypeName).foreach(ds.removeSchema)
        }
      }
    }

    "serialize updates from different data stores" in {
      WithClose(datastore(), datastore()) { (ds, ds2) =>
        testSchema(ds, "update-schema-concurrent") { sft =>
          def previous(): Map[String, String] = {
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              UserDataTable.read(cx, this.schema, Seq(SftUserData.PreviousView)).map { case (view, _, prev) => view -> prev }.toMap
            }
          }
          // gets the views for each generation, starting with the current one
          def generations(): List[String] = {
            val chain = previous()
            def next(view: String): List[String] = view :: chain.get(view).toList.flatMap(next)
            next(typeInfo(ds, sft.getTypeName).tables.view.name.raw)
          }

          // update the schema through a store that has an out-of-date schema cached
          ds2.getSchema(sft.getTypeName) must not(beNull)
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, s"$updatedSpec,extra1:String"))
          ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, s"$updatedSpec,extra1:String,extra2:String"))
          foreach(Seq(ds, ds2)) { ds =>
            ds.getTypeNames.toSeq.filter(_ == sft.getTypeName) must haveLength(1)
          }
          generations() must haveLength(3)
          generations().last mustEqual sft.getTypeName

          // update the schema at the same time, through different stores
          val stores = Seq.fill(4)(datastore())
          try {
            val updates = stores.zipWithIndex.map { case (store, i) =>
              val spec = s"$updatedSpec,extra1:String,extra2:String,concurrent$i:String"
              CachedThreadPool.submit(() => store.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, spec)))
            }
            foreach(updates)(_.get(1, TimeUnit.MINUTES) must not(throwAn[Exception]))
          } finally {
            stores.foreach(_.dispose())
          }
          WithClose(datastore()) { ds3 =>
            ds3.getTypeNames.toSeq.filter(_ == sft.getTypeName) must haveLength(1)
            attributes(ds3.getSchema(sft.getTypeName)).count(_.startsWith("concurrent")) mustEqual 1
          }
          // each update is based on the one before it
          generations() must haveLength(7)
          generations().last mustEqual sft.getTypeName
          previous().values.toSeq.count(generations().contains) mustEqual 6
        }
      }
    }

    "update the main view without privileges to create temporary views" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-temp-privilege") { sft =>
          val info = TypeInfo(this.schema, sft)
          val view = info.tables.view.name.qualified
          val role = "update_schema_no_temp"
          def execute(cx: Connection, statements: String*): Unit =
            WithClose(cx.createStatement())(st => statements.foreach(st.execute))
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val database = WithClose(cx.createStatement()) { st =>
              WithClose(st.executeQuery("SELECT quote_ident(current_database())")) { rs => rs.next(); rs.getString(1) }
            }
            execute(cx,
              s"CREATE ROLE $role",
              s"GRANT CREATE ON SCHEMA ${info.schema.quoted} TO $role",
              s"REVOKE TEMPORARY ON DATABASE $database FROM PUBLIC",
              s"ALTER VIEW $view OWNER TO $role")
            try {
              execute(cx, s"SET ROLE $role")
              try {
                execute(cx, "CREATE TEMP VIEW update_schema_no_temp_view AS SELECT 1") must throwA[java.sql.SQLException]
                implicit val ex: ExecutionContext = new ExecutionContext(cx)
                try { MainView.create(info) } finally {
                  ex.close()
                }
              } finally {
                execute(cx, "RESET ROLE")
              }
            } finally {
              execute(cx,
                s"ALTER VIEW $view OWNER TO CURRENT_USER",
                s"GRANT TEMPORARY ON DATABASE $database TO PUBLIC",
                s"REVOKE CREATE ON SCHEMA ${info.schema.quoted} FROM $role",
                s"DROP ROLE $role")
            }
          }
          // verify things still work
          val features = writeFeatures(ds, ds.getSchema(sft.getTypeName))
          query(ds, sft.getTypeName).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
        }
      }
    }

    "update user data configuration" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-userdata") { sft =>
          SftUserData.MaxPartitions.get(sft) must beNone
          val updated = SimpleFeatureTypes.copy(sft)
          updated.getUserData.put(SftUserData.MaxPartitions.key, "10")
          updated.getUserData.put(SftUserData.IntervalHours.key, "2")
          updated.getUserData.put(SftUserData.CronMinute.key, "3")
          ds.updateSchema(sft.getTypeName, updated)

          foreach(Seq(ds.getSchema(sft.getTypeName), WithClose(datastore())(_.getSchema(sft.getTypeName)))) { result =>
            SimpleFeatureTypes.encodeType(result) mustEqual SimpleFeatureTypes.encodeType(sft)
            SftUserData.MaxPartitions.get(result) must beSome(10)
            SftUserData.IntervalHours.get(result) mustEqual 2
            SftUserData.CronMinute.get(result) must beSome(3)
          }
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            UserDataTable.read(cx, this.schema, sft.getTypeName) must containAllOf(
              Seq(SftUserData.MaxPartitions.key -> "10", SftUserData.IntervalHours.key -> "2", SftUserData.CronMinute.key -> "3"))
            val sql = s"SELECT schedule FROM cron.job WHERE jobname = ${PartitionMaintenance.jobName(TypeInfo(this.schema, sft)).quoted}"
            WithClose(cx.createStatement()) { st =>
              WithClose(st.executeQuery(sql)) { rs =>
                rs.next() must beTrue
                rs.getString(1) mustEqual "3,13,23,33,43,53 * * * *"
              }
            }
          }
          // verify things still work
          val features = writeFeatures(ds, ds.getSchema(sft.getTypeName))
          query(ds, sft.getTypeName).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
        }
      }
    }

    "update user data configuration without waiting for queries against the view" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-userdata-locks") { sft =>
          val info = TypeInfo(this.schema, sft)
          writeFeatures(ds, sft)
          def viewId(): Long = {
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement()) { st =>
                // note: xmin changes if the view is replaced
                WithClose(st.executeQuery(s"SELECT xmin::text::bigint FROM pg_class WHERE oid = ${info.tables.view.name.asRegclass}")) { rs =>
                  rs.next() must beTrue
                  rs.getLong(1)
                }
              }
            }
          }
          val before = viewId()
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            cx.setAutoCommit(false)
            var future: Future[_] = null
            try {
              // simulates a long-running query against the view - note that this also locks all the underlying tables
              WithClose(cx.createStatement()) { st =>
                st.execute(s"LOCK TABLE ${info.tables.view.name.qualified} IN ACCESS SHARE MODE")
                // simulates long-running partition maintenance
                Seq(info.tables.writeAheadPartitions, info.tables.mainPartitions, info.tables.spillPartitions).foreach { t =>
                  st.execute(s"LOCK TABLE ONLY ${t.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE")
                }
              }
              future = CachedThreadPool.submit(() => {
                WithClose(datastore()) { ds2 =>
                  val updated = SimpleFeatureTypes.copy(ds2.getSchema(sft.getTypeName))
                  updated.getUserData.put(SftUserData.MaxPartitions.key, "10")
                  ds2.updateSchema(sft.getTypeName, updated)
                  ds2.upgrade(ds2.getSchema(sft.getTypeName))
                }
              })
              future.get(60, TimeUnit.SECONDS)
            } finally {
              cx.rollback()
              cx.setAutoCommit(true)
              if (future != null) {
                Try(future.get(60, TimeUnit.SECONDS))
              }
            }
          }
          WithClose(datastore())(ds2 => SftUserData.MaxPartitions.get(ds2.getSchema(sft.getTypeName)) must beSome(10))
          viewId() mustEqual before
          query(ds, sft.getTypeName) must haveLength(10)
        }
      }
    }

    "update schemas with a connection pool of two connections" in {
      // note: if an update needed more connections, it would fail after the connection timeout
      WithClose(datastore(params ++ Map("max connections" -> "2", "Connection timeout" -> "10"))) { ds =>
        testSchema(ds, "update-schema-pool") { sft =>
          writeFeatures(ds, sft)
          // create a new generation
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
          val updated = ds.getSchema(sft.getTypeName)
          attributes(updated) mustEqual attributes(SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))
          // update the configuration of a type that has a previous generation
          // note: use a new data store, so that the previous generation hasn't already been loaded
          WithClose(datastore(params ++ Map("max connections" -> "2", "Connection timeout" -> "10"))) { ds2 =>
            val config = SimpleFeatureTypes.copy(ds2.getSchema(sft.getTypeName))
            config.getUserData.put(SftUserData.MaxPartitions.key, "10")
            ds2.updateSchema(sft.getTypeName, config)
            // rename
            val renamed = s"${sft.getTypeName}-renamed"
            ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(ds2.getSchema(sft.getTypeName), renamed))
            ds2.updateSchema(renamed, SimpleFeatureTypes.renameSft(ds2.getSchema(renamed), sft.getTypeName))
          }
          WithClose(datastore())(ds2 => SftUserData.MaxPartitions.get(ds2.getSchema(sft.getTypeName)) must beSome(10))
          query(ds, sft.getTypeName) must haveLength(10)
          // upgrade a type that has a previous generation
          // note: use a new data store, so that the previous generation hasn't already been loaded
          WithClose(datastore(params ++ Map("max connections" -> "2", "Connection timeout" -> "10"))) { ds2 =>
            val config = SimpleFeatureTypes.copy(ds2.getSchema(sft.getTypeName))
            config.getUserData.put(SftUserData.MaxPartitions.key, "20")
            ds2.upgrade(config)
          }
          WithClose(datastore())(ds2 => SftUserData.MaxPartitions.get(ds2.getSchema(sft.getTypeName)) must beSome(20))
          query(ds, sft.getTypeName) must haveLength(10)
        }
      }
    }

    "read and write, but fail schema operations, with a connection pool of one connection" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-pool-single") { sft =>
          WithClose(datastore(params ++ Map("max connections" -> "1", "Connection timeout" -> "10"))) { single =>
            // reading and writing only require a single connection
            writeFeatures(single, sft)
            query(single, sft.getTypeName) must haveLength(10)
            // schema operations require two connections, and fail up front instead of waiting for the connection timeout
            single.createSchema(this.sft("update-schema-pool-single-create")) must throwAn[IllegalStateException]
            single.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec)) must
                throwAn[IllegalStateException]
            single.upgrade(single.getSchema(sft.getTypeName)) must throwAn[IllegalStateException]
            single.removeSchema(sft.getTypeName) must throwAn[IllegalStateException]
          }
          ds.getTypeNames.toSeq must not(contain("update-schema-pool-single-create"))
          attributes(ds.getSchema(sft.getTypeName)) mustEqual attributes(sft)
          query(ds, sft.getTypeName) must haveLength(10)
        }
      }
    }

    "keep an explicit default geometry that isn't the first geometry" in {
      val geomSpec = spec.replace("*geom:Point:srid=4326", "geom:Point:srid=4326,*geom2:Point:srid=4326")
      // removes the 'age' attribute and adds a new one
      val geomUpdatedSpec = geomSpec.replace("age:Int,", "").replace(";", ",extra:String;")
      def geomFeatures(sft: SimpleFeatureType, prefix: String): Seq[SimpleFeature] = Seq.tabulate(10) { i =>
        val builder = new SimpleFeatureBuilder(sft)
        builder.set("name", java.util.Collections.singletonList(s"$prefix$i"))
        builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
        builder.set("geom", WKTUtils.read(s"POINT(0 $i)"))
        builder.set("geom2", WKTUtils.read(s"POINT(10 $i)"))
        val sf = builder.buildFeature(s"$prefix$i")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }
      // a filter against the default geometry
      val ff = CommonFactoryFinder.getFilterFactory
      val bbox = ff.bbox(ff.property(""), new ReferencedEnvelope(9, 11, -1, 4.5, CRS_EPSG_4326))
      def queryDefaultGeom(ds: DataStore, typeName: String): Seq[SimpleFeature] = {
        val fromReader = WithClose(ds.getFeatureReader(new Query(typeName, bbox), Transaction.AUTO_COMMIT)) { reader =>
          CloseableIterator(reader).toList.sortBy(_.getID)
        }
        val fromSource = CloseableIterator(ds.getFeatureSource(typeName).getFeatures(bbox).features()).toList.sortBy(_.getID)
        fromSource.map(compFromDb) mustEqual fromReader.map(compFromDb)
        foreach(fromReader) { sf =>
          sf.getFeatureType.getGeometryDescriptor.getLocalName mustEqual "geom2"
          sf.getDefaultGeometry mustEqual sf.getAttribute("geom2")
        }
        fromReader
      }
      // verifies that the partition procedures are sorting on the expected column
      def sortColumn(ds: PartitionedPostgisDataStore, info: TypeInfo): Seq[String] = {
        WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
          WithClose(cx.prepareStatement("SELECT prosrc FROM pg_proc WHERE proname = ?")) { st =>
            st.setString(1, CompactPartitions.name(info).raw)
            WithClose(st.executeQuery()) { rs =>
              rs.next() must beTrue
              "_st_sortablehash\\(\"(\\w+)\"\\)".r.findAllMatchIn(rs.getString(1)).map(_.group(1)).toList.distinct
            }
          }
        }
      }

      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-default-geom", geomSpec) { sft =>
          foreach(Seq(sft, WithClose(datastore())(_.getSchema(sft.getTypeName)))) { result =>
            attributes(result) mustEqual Seq("name", "props", "age", "dtg", "geom", "geom2")
            result.getGeometryDescriptor.getLocalName mustEqual "geom2"
            SftUserData.GeomField.get(result) must beSome("geom2")
          }
          val original = TypeInfo(this.schema, sft)
          original.cols.geom.raw mustEqual "geom2"
          sortColumn(ds, original) mustEqual Seq("geom2")

          val features = geomFeatures(sft, "fid")
          write(ds, features)
          queryDefaultGeom(ds, sft.getTypeName).map(_.getID) mustEqual features.take(5).map(_.getID)
          WithClose(ds.getFeatureWriter(sft.getTypeName, bbox, Transaction.AUTO_COMMIT)) { writer =>
            writer.hasNext must beTrue
            writer.next().getFeatureType.getGeometryDescriptor.getLocalName mustEqual "geom2"
          }
          ds.getFeatureSource(sft.getTypeName).getCount(new Query(sft.getTypeName, bbox)) mustEqual 5

          // re-creating the procedures keeps the default geometry
          ds.upgrade(ds.getSchema(sft.getTypeName))
          sortColumn(ds, original) mustEqual Seq("geom2")
          val config = SimpleFeatureTypes.copy(ds.getSchema(sft.getTypeName))
          config.getUserData.put(SftUserData.MaxPartitions.key, "10")
          ds.updateSchema(sft.getTypeName, config)
          sortColumn(ds, original) mustEqual Seq("geom2")
          foreach(Seq(ds.getSchema(sft.getTypeName), WithClose(datastore())(_.getSchema(sft.getTypeName)))) { result =>
            result.getGeometryDescriptor.getLocalName mustEqual "geom2"
            SftUserData.MaxPartitions.get(result) must beSome(10)
          }
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            UserDataTable.read(cx, this.schema, sft.getTypeName) must havePair(SftUserData.GeomField.key -> "geom2")
            runMaintenance(cx, original)
          }
          queryDefaultGeom(ds, sft.getTypeName).map(_.getID) mustEqual features.take(5).map(_.getID)

          // the default geometry can't be changed
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, geomSpec.replace("*geom2", "geom2"))) must
              throwAn[UnsupportedOperationException]
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, geomUpdatedSpec.replace("*geom2", "geom2"))) must
              throwAn[UnsupportedOperationException]

          // adding and removing attributes keeps the default geometry
          ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, geomUpdatedSpec))
          val current = typeInfo(ds, sft.getTypeName)
          current.tables.view must not(beEqualTo(original.tables.view))
          current.cols.geom.raw mustEqual "geom2"
          sortColumn(ds, current) mustEqual Seq("geom2")
          val updated = ds.getSchema(sft.getTypeName)
          foreach(Seq(updated, WithClose(datastore())(_.getSchema(sft.getTypeName)))) { result =>
            attributes(result) mustEqual Seq("name", "props", "dtg", "geom", "geom2", "extra")
            result.getGeometryDescriptor.getLocalName mustEqual "geom2"
          }
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            UserDataTable.read(cx, this.schema, current.typeName) must havePair(SftUserData.GeomField.key -> "geom2")
          }
          queryDefaultGeom(ds, sft.getTypeName).map(_.getID) mustEqual features.take(5).map(_.getID)

          // features that were created with the feature type from before the update are written by attribute name
          val stale = geomFeatures(sft, "stale")
          ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, stale.asJava)).asScala.map(_.getID) mustEqual
              stale.map(_.getID)
          val written = query(ds, sft.getTypeName, "name = 'stale3'")
          written.map(compFromDb) mustEqual Seq(compWithFid(stale(3), sft, Some(attributes(updated).filter(_ != "extra").toArray)) ++ Seq(null))
          queryDefaultGeom(ds, sft.getTypeName).map(_.getID) mustEqual (features.take(5) ++ stale.take(5)).map(_.getID)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT))(runMaintenance(_, original, current))
          queryDefaultGeom(ds, sft.getTypeName).map(_.getID) mustEqual (features.take(5) ++ stale.take(5)).map(_.getID)
        }
      }
    }

    "upgrade schemas using a feature type that wasn't loaded from the data store" in {
      WithClose(datastore()) { ds =>
        // note: type names with more than 31 characters use a generated alias for their tables
        testSchema(ds, "update-schema-upgrade-with-a-long-type-name") { sft =>
          val info = TypeInfo(this.schema, sft)
          info.typeIdentifier must not(beEqualTo(sft.getTypeName))
          writeFeatures(ds, sft)
          val config = SimpleFeatureTypes.createType(sft.getTypeName, spec)
          config.getUserData.put(SftUserData.MaxPartitions.key, "10")
          // an index can't be added to an existing attribute
          val indexed = SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("age:Int", "age:Int:index=true"))
          indexed.getUserData.put(SftUserData.MaxPartitions.key, "5")
          ds.upgrade(indexed) must throwAn[UnsupportedOperationException]
          SftUserData.MaxPartitions.get(ds.getSchema(sft.getTypeName)) must beNone
          ds.upgrade(config)
          WithClose(datastore()) { ds2 =>
            val upgraded = ds2.getSchema(sft.getTypeName)
            SftUserData.MaxPartitions.get(upgraded) must beSome(10)
            // the existing tables are still being used
            TypeInfo(this.schema, upgraded).typeIdentifier mustEqual info.typeIdentifier
            query(ds2, sft.getTypeName) must haveLength(10)
          }
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            UserDataTable.read(cx, this.schema, Seq(SftUserData.IdentAlias)).filter(_._1 == sft.getTypeName).map(_._3) mustEqual
                Seq(info.typeIdentifier)
          }
        }
      }
    }

    "recover the default geometry for schemas created before it was recorded" in {
      val geomSpec = spec.replace("*geom:Point:srid=4326", "geom:Point:srid=4326,*geom2:Point:srid=4326")
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-legacy-geom", geomSpec) { sft =>
          def recorded(): Seq[String] = {
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              UserDataTable.read(cx, this.schema, Seq(SftUserData.GeomField)).filter(_._1 == sft.getTypeName).map(_._3)
            }
          }
          recorded() mustEqual Seq("geom2")
          // simulate a schema created by an older version, which didn't record the default geometry
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val table = TableIdentifier(this.schema, UserDataTable.Name.raw)
            val delete = s"DELETE FROM ${table.qualified} WHERE type_name = ? AND key = ?"
            WithClose(cx.prepareStatement(delete)) { st =>
              st.setString(1, sft.getTypeName)
              st.setString(2, SftUserData.GeomField.key)
              st.executeUpdate() mustEqual 1
            }
          }
          recorded() must beEmpty
          WithClose(datastore()) { ds2 =>
            // the default geometry is determined from the existing procedures
            val loaded = ds2.getSchema(sft.getTypeName)
            loaded.getGeometryDescriptor.getLocalName mustEqual "geom2"
            // the schema can be updated using the original default geometry, but the default geometry can't be changed
            val config = SimpleFeatureTypes.createType(sft.getTypeName, geomSpec)
            config.getUserData.put(SftUserData.MaxPartitions.key, "10")
            ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, spec.replace(":srid=4326", ":srid=4326,geom2:Point:srid=4326"))) must
              throwAn[UnsupportedOperationException]
            ds2.updateSchema(sft.getTypeName, config)
            SftUserData.MaxPartitions.get(ds2.getSchema(sft.getTypeName)) must beSome(10)
          }
          // updating the schema records the default geometry
          recorded() mustEqual Seq("geom2")
          WithClose(datastore())(_.getSchema(sft.getTypeName).getGeometryDescriptor.getLocalName mustEqual "geom2")
        }
      }
    }

    "run concurrent schema operations with a connection pool of two connections" in {
      // note: if the operations starved each other of connections, they would fail after the connection timeout
      WithClose(datastore(params ++ Map("max connections" -> "2", "Connection timeout" -> "10"))) { ds =>
        val sfts = Seq.tabulate(6)(i => this.sft(s"update-schema-pool-concurrent-$i"))
        try {
          val creates = sfts.map(sft => CachedThreadPool.submit(() => ds.createSchema(sft)))
          foreach(creates)(_.get(1, TimeUnit.MINUTES) must not(throwAn[Exception]))
          ds.getTypeNames.toSeq must containAllOf(sfts.map(_.getTypeName))
          val removes = sfts.map(sft => CachedThreadPool.submit(() => ds.removeSchema(sft.getTypeName)))
          foreach(removes)(_.get(1, TimeUnit.MINUTES) must not(throwAn[Exception]))
          ds.getTypeNames.toSeq.intersect(sfts.map(_.getTypeName)) must beEmpty
        } finally {
          ds.getTypeNames.filter(n => sfts.exists(_.getTypeName == n)).foreach(ds.removeSchema)
        }
      }
    }

    "load schemas that were created or renamed through a different data store" in {
      WithClose(datastore()) { ds =>
        val sft = this.sft("update-schema-load")
        val renamed = s"${sft.getTypeName}-renamed"
        // load the mapping of feature type names to views, so that it's cached
        ds.getTypeNames
        ds.getSchema(sft.getTypeName) must throwAn[java.io.IOException]
        try {
          WithClose(datastore()) { ds2 =>
            ds2.createSchema(sft)
            ds.getSchema(sft.getTypeName) must not(beNull)
            ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(ds2.getSchema(sft.getTypeName), renamed))
          }
          // note: the feature type is still cached under the old name
          val loaded = ds.getSchema(renamed)
          loaded must not(beNull)
          loaded.getTypeName mustEqual renamed
          attributes(loaded) mustEqual attributes(sft)
        } finally {
          WithClose(datastore())(ds2 => typeNames(ds2, sft.getTypeName).foreach(ds2.removeSchema))
        }
      }
    }

    "serialize removing and updating a schema from different data stores" in {
      foreach(0 until 3) { i =>
        WithClose(datastore(), datastore()) { (ds, ds2) =>
          val sft = this.sft(s"update-schema-remove-$i")
          ds.createSchema(sft)
          try {
            ds2.getSchema(sft.getTypeName) must not(beNull)
            // note: the update will fail if the schema is removed first
            val update =
              CachedThreadPool.call(() => Try(ds2.updateSchema(sft.getTypeName, SimpleFeatureTypes.createType(sft.getTypeName, updatedSpec))))
            val remove = CachedThreadPool.submit(() => ds.removeSchema(sft.getTypeName))
            remove.get(1, TimeUnit.MINUTES) must not(throwAn[Exception])
            update.get(1, TimeUnit.MINUTES) must not(throwAn[Exception])
            // regardless of the order, there shouldn't be anything left
            WithClose(datastore())(_.getTypeNames.toSeq must not(contain(sft.getTypeName)))
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.prepareStatement("SELECT relname FROM pg_class WHERE starts_with(relname, ?)")) { st =>
                st.setString(1, sft.getTypeName)
                WithClose(st.executeQuery()) { rs =>
                  Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList must beEmpty
                }
              }
            }
          } finally {
            ds.getTypeNames.filter(_ == sft.getTypeName).foreach(ds.removeSchema)
          }
        }
      }
    }

    "serialize creating a schema and renaming a schema to the same name" in {
      foreach(0 until 3) { i =>
        WithClose(datastore(), datastore()) { (ds, ds2) =>
          val sft = this.sft(s"update-schema-rename-$i")
          val renamed = this.sft(s"update-schema-rename-$i-to")
          ds.createSchema(sft)
          try {
            val rename =
              CachedThreadPool.call(() => Try(ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(sft, renamed.getTypeName))))
            val create = CachedThreadPool.call(() => Try(ds2.createSchema(renamed)))
            // only one of them can use the name
            Seq(rename, create).map(_.get(1, TimeUnit.MINUTES)).count(_.isSuccess) mustEqual 1
            WithClose(datastore()) { ds3 =>
              ds3.getTypeNames.toSeq.count(_ == renamed.getTypeName) mustEqual 1
              ds3.getSchema(renamed.getTypeName) must not(beNull)
            }
          } finally {
            WithClose(datastore()) { ds3 =>
              ds3.getTypeNames.filter(Seq(sft.getTypeName, renamed.getTypeName).contains).foreach(ds3.removeSchema)
            }
          }
        }
      }
    }

    "reject unsupported schema updates" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "update-schema-invalid") { sft =>
          def update(fn: SimpleFeatureTypeBuilder => Unit): SimpleFeatureType = {
            val builder = new SimpleFeatureTypeBuilder()
            builder.init(sft)
            fn(builder)
            val updated = builder.buildFeatureType()
            updated.getUserData.putAll(sft.getUserData)
            updated
          }
          def userData(key: String, value: String): SimpleFeatureType = {
            val updated = SimpleFeatureTypes.copy(sft)
            updated.getUserData.put(key, value)
            updated
          }

          ds.updateSchema("update-schema-does-not-exist", sft) must throwAn[java.io.IOException]
          val unsupported = Seq(
            // re-ordering attributes
            SimpleFeatureTypes.createType(sft.getTypeName, "props:String:json=true,name:List[String],age:Int,dtg:Date,*geom:Point:srid=4326"),
            // adding an attribute before existing attributes
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("age:Int", "count:Long,age:Int")),
            // changing an attribute type
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("age:Int", "age:Long")),
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("geom:Point", "geom:Geometry")),
            update { b =>
              b.remove("geom")
              b.userData(org.geotools.jdbc.JDBCDataStore.JDBC_NATIVE_SRID, Int.box(3857)).add("geom", classOf[org.locationtech.jts.geom.Point])
              b.setDefaultGeometry("geom")
            },
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("props:String:json=true", "props:String")),
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("name:List[String]", "name:List[Integer]")),
            // adding an index to an existing attribute
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("age:Int", "age:Int:index=true")),
            // adding a required attribute
            update { b => b.minOccurs(1).nillable(false); b.add("required", classOf[String]) },
            // removing the default geometry
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("*geom:Point:srid=4326", "*geom2:Point:srid=4326")),
            // changing the default geometry
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("*geom:Point:srid=4326", "geom:Point:srid=4326,*geom2:Point:srid=4326")),
            // removing the default date
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("dtg:Date,", "").replace("srid=4326;", "srid=4326,dtg2:Date;")),
            // changing the default date
            SimpleFeatureTypes.createType(sft.getTypeName, spec.replace("srid=4326;", "srid=4326,dtg2:Date;") + ",geomesa.index.dtg=dtg2"),
            // changing configuration that is fixed when the schema is created
            userData(SftUserData.PagesPerRange.key, "32"),
            userData(SftUserData.WalLogEnabled.key, "false"),
            userData(SftUserData.VisEnabled.key, "true"),
            userData(SftUserData.CoveringDtgIndex.key, "true"),
          )
          foreach(unsupported) { updated =>
            ds.updateSchema(sft.getTypeName, updated) must throwAn[UnsupportedOperationException]
          }
          // verify the schema wasn't modified
          SimpleFeatureTypes.encodeType(ds.getSchema(sft.getTypeName), includeUserData = true) mustEqual
              SimpleFeatureTypes.encodeType(sft, includeUserData = true)
        }
      }
    }
  }
}
