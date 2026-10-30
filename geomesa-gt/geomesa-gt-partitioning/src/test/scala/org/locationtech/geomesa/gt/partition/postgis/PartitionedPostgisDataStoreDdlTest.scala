/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.apache.commons.codec.digest.MurmurHash3
import org.geotools.api.data._
import org.geotools.data._
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect._
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{PartitionTablespacesTable, PrimaryKeyTable, SequenceTable, UserDataTable}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.matcher.MatchResult

import java.nio.charset.StandardCharsets
import java.util.concurrent._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class PartitionedPostgisDataStoreDdlTest extends TestWithSharedPostgisContainer {

  import scala.collection.JavaConverters._

  "PartitionedPostgisDataStore" should {

    "fail with a useful error message if type name is too long" in {
      WithClose(datastore()) { ds =>
        // This sft name exceeds 63 characters, so it should fail
        val typeName = "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz"
        ds.createSchema(SimpleFeatureTypes.createType(typeName, spec)) must throwAn[java.io.IOException].like {
          case e => e.getCause.getMessage mustEqual "Can't create schema: type name exceeds max supported Postgres identifier length of 63"
        }
      }
    }

    "serialize concurrent schema creation" in {
      WithClose(datastore()) { ds =>
        val sft = this.sft("concurrent-schema-create")
        val lockId = {
          val bytes = s"$schema.${sft.getTypeName}".getBytes(StandardCharsets.UTF_8)
          MurmurHash3.hash128x64(bytes, 0, bytes.length, 0).head
        }
        WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
          WithClose(cx.createStatement())(_.execute(s"SELECT pg_advisory_lock($lockId)"))
          var future: Future[_] = null
          try {
            future = CachedThreadPool.submit(() => WithClose(datastore())(_.createSchema(sft)) )
            var waiting = false
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (!waiting && System.nanoTime() < deadline) {
              WithClose(cx.prepareStatement(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE locktype = 'advisory' AND classid::int = ? " +
                    "AND objid::int = ? AND NOT granted)")) { st =>
                st.setInt(1, (lockId >>> 32).toInt) // the lock id gets split into two ints in the pg_locks table
                st.setInt(2, lockId.toInt)
                WithClose(st.executeQuery()) { rs =>
                  rs.next()
                  waiting = rs.getBoolean(1)
                }
              }
              if (!waiting) {
                Thread.sleep(10)
              }
            }
            waiting must beTrue
            future.isDone must beFalse
          } finally {
            WithClose(cx.createStatement())(_.execute(s"SELECT pg_advisory_unlock($lockId)"))
          }
          future.get(10, TimeUnit.SECONDS)
          future.isDone must beTrue
        }
        ds.getTypeNames.toSeq must contain(sft.getTypeName)
      }
    }

    "re-create functions" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "recreate") { sft =>
          val features = writeFeatures(ds, sft)

          // verify data comes back
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            val result = CloseableIterator(reader).toList
            result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
          }

          val typeInfo = TypeInfo(this.schema, sft)

          // replace the age-off function so that we can verify it gets updated later
          val oldAgeOff = DropAgedOffPartitions.name(typeInfo)
          val body =
            s"""    BEGIN
               |      SELECT value::int FROM ${typeInfo.schema.quoted}.${UserDataTable.Name.quoted};
               |    END;""".stripMargin
          val sql =
            s"""CREATE OR REPLACE PROCEDURE ${oldAgeOff.quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
               |  $$BODY$$
               |$body
               |  $$BODY$$;
               |""".stripMargin

          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement(sql))(_.executeUpdate())
            WithClose(cx.prepareStatement(s"SELECT prosrc FROM pg_proc WHERE proname = ${oldAgeOff.asLiteral};")) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                rs.getString(1).trim mustEqual body.trim
              }
            }
            // now drop the main view
            WithClose(cx.prepareStatement(s"""DROP VIEW "${sft.getTypeName}""""))(_.executeUpdate())
          }

          // verify the feature type no longer returns
          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          // re-create the schema, adding some extra user data
          sft.getUserData.put("pg.partitions.max", "2")
          // we have to get a new data store so that it doesn't use the cached entry...
          WithClose(datastore()) { ds =>
            ds.createSchema(sft)
            val schema = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
            schema must not(beNull)
            schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)

            // verify data still comes back
            WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
              val result = CloseableIterator(reader).toList
              result.sortBy(_.getID).map(compFromDb) mustEqual features.map(compWithFid(_, sft))
            }
          }

          // verify that the age-off function was re-created
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement(s"SELECT prosrc FROM pg_proc WHERE proname = ${oldAgeOff.asLiteral};")) { st =>
              WithClose(st.executeQuery()) { rs =>
                rs.next() must beTrue
                rs.getString(1).trim must not(beEqualTo(body.trim))
              }
            }
          }
        }
      }
    }

    "drop all associated tables on removeSchema" in {
      WithClose(datastore()) { ds =>

        // get all the tables associated with the schema
        def getTablesAndIndices: Seq[String] = {
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.getMetaData.getTables(null, null, "dropme%", null)) { rs =>
              Iterator.continually(rs).takeWhile(_.next()).map(_.getString(3)).toList
            }
          }
        }
        // get all the procedures and functions associated with the schema
        def getFunctions: Seq[String] = {
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val procedures = WithClose(cx.getMetaData.getProcedures(null, null, "%dropme%")) { rs =>
              Iterator.continually(rs).takeWhile(_.next()).map(_.getString(3)).toList
            }
            val functions = WithClose(cx.getMetaData.getFunctions(null, null, "%dropme%")) { rs =>
              Iterator.continually(rs).takeWhile(_.next()).map(_.getString(3)).toList
            }
            procedures ++ functions
          }
        }
        // get all the scheduled cron jobs associated with the schema
        def getCrons: Seq[String] = {
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.prepareStatement("SELECT command from cron.job where command like '%dropme%';")) { st =>
              WithClose(st.executeQuery()) { rs =>
                Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
              }
            }
          }
        }
        // get all the user data and other associated metadata
        def getMeta: Map[String, Seq[String]] = {
          val meta = WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            Seq(
              (UserDataTable.Name, "type_name", "key"),
              (SequenceTable.Name, "type_name", "value"),
              (PrimaryKeyTable.Name, "table_name", "pk_column"),
              (PartitionTablespacesTable.Name, "type_name", "table_type")
            ).map { case (table, where, select) =>
              if (WithClose(cx.getMetaData.getTables(null, null, table.raw, null))(_.next())) {
                WithClose(cx.prepareStatement(s"SELECT $select FROM ${table.quoted} WHERE $where like 'dropme%';")) { st =>
                  WithClose(st.executeQuery()) { rs =>
                    table.raw -> Iterator.continually(rs).takeWhile(_.next()).map(rs => s"${table.raw} ${rs.getString(1)}").toList
                  }
                }
              } else {
                table.raw -> List.empty
              }
            }
          }
          meta.filter(_._2.nonEmpty).toMap
        }

        foreach(Seq("dropme-test", "dropmetest")) { name =>
          testSchema(ds, name) { sft =>
            writeFeatures(ds, sft)
            // _wa, _wa_partition, _partition, _spill tables + dtg, pk, geom indices for each
            // _analyze_queue, _sort_queue, _wa_000, _wa_migrations, _age_off_queue, _wa_retirements, main view
            getTablesAndIndices must haveLength(28)
            // delete/insert/update/wa triggers
            // analyze_partitions, clean_wa_migrations, clean_wa_retirements, clean_age_off, compact, drop_age_off, merge_wa, part_maintenance, part_wa, roll_wa,
            getFunctions must haveLength(14)
            // log_cleaner, analyze_partitions, clean_wa_migrations, clean_wa_retirements, clean_age_off, roll_wa, partition_maintenance
            getCrons must haveLength(7)
            // 4 user data, 1 seq count, 1 primary key
            val meta = getMeta
            meta must haveSize(3)
            meta.get(UserDataTable.Name.raw) must beSome(haveLength[Seq[String]](4))
            meta.get(SequenceTable.Name.raw) must beSome(haveLength[Seq[String]](1))
            meta.get(PrimaryKeyTable.Name.raw) must beSome(haveLength[Seq[String]](1))
          }
          getTablesAndIndices must beEmpty
          getFunctions must beEmpty
          getCrons must beEmpty
          getMeta must beEmpty
        }
      }
    }

    "create child partitions logged or unlogged according to pg.wal.enabled" in {
      WithClose(datastore()) { ds =>
        foreach(Seq(true, false)) { logged =>
          val name = s"child_wal_${if (logged) "logged" else "unlogged"}"
          val spec = if (logged) { this.spec } else { s"${this.spec},${SftUserData.WalLogEnabled.key}=false" }
          testSchema(ds, name, spec) { sft =>
            writeFeatures(ds, sft)
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              val typeInfo = TypeInfo(this.schema, sft)
              WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
              WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())

              // the tables that hold the rows, i.e. the children of the partitioned parents
              def persistence(parent: TableConfig): Seq[String] = {
                val sql =
                  s"""SELECT c.relpersistence FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                     |  WHERE i.inhparent = ${parent.name.asRegclass}""".stripMargin
                WithClose(cx.createStatement().executeQuery(sql)) { rs =>
                  Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
                }
              }
              val expected = if (logged) { "p" } else { "u" }
              // the features span 20 to 200 minutes ago: the older ones go straight to main partitions, the
              // newer ones to write ahead partitions
              val writeAhead = persistence(typeInfo.tables.writeAheadPartitions)
              val direct = persistence(typeInfo.tables.mainPartitions)
              writeAhead must not(beEmpty)
              direct must not(beEmpty)
              foreach(writeAhead ++ direct)(_ mustEqual expected)

              // most main partitions are made by merging write ahead partitions once they pass the cutoff - run
              // it as if a few hours had passed
              WithClose(cx.prepareCall(s"call ${MergeWriteAheadPartitions.name(typeInfo).quoted}(now()::timestamp + interval '3 hours');"))(_.execute())
              val deferred = persistence(typeInfo.tables.writeAheadPartitions)
              deferred must not(beEmpty)
              foreach(deferred)(_ mustEqual expected)
              val merged = persistence(typeInfo.tables.mainPartitions)
              merged.length must beGreaterThan(direct.length)
              foreach(merged)(_ mustEqual expected)
            }
          }
        }
      }
    }

    "create logged tables" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "logged_test") { sft =>
          val typeInfo = TypeInfo(this.schema, sft)
          val tables =
            Seq(
              typeInfo.tables.mainPartitions.name.raw,
              typeInfo.tables.writeAheadPartitions.name.raw,
              typeInfo.tables.spillPartitions.name.raw,
              typeInfo.tables.analyzeQueue.name.raw,
              typeInfo.tables.sortQueue.name.raw,
            )
          forall(tables) { tableName =>
            val sql = isTableLoggedQuery(tableName, "public")
            // verify that the table is logged
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement()) { st =>
                WithClose(st.executeQuery(sql)) { rs =>
                  rs.next() must beTrue
                  val tableType = rs.getString("table_type")
                  logger.debug(s"Table ${rs.getString("table_name")} is $tableType")
                  tableType mustEqual "permanent"
                }
              }
            }
          }
        }
      }
    }

    "create unlogged tables" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "unlogged_test", s"$spec,${SftUserData.WalLogEnabled.key}=false") { sft =>
          SftUserData.WalLogEnabled.get(sft) must beFalse

          val typeInfo = TypeInfo(this.schema, sft)
          val tables =
            Seq(
              typeInfo.tables.mainPartitions.name.raw,
              typeInfo.tables.writeAheadPartitions.name.raw,
              // typeInfo.tables.writeAhead.name.raw, write ahead table is created with PartitionedPostgisDialect#encodePostCreateTable
              //  which doesn't have access to the user data, should be ok because the write ahead main table doesn't have any data
              typeInfo.tables.spillPartitions.name.raw,
              typeInfo.tables.analyzeQueue.name.raw,
              typeInfo.tables.sortQueue.name.raw,
            )
          forall(tables) { tableName =>
            val sql = isTableLoggedQuery(tableName, "public")
            // verify that the table is unlogged
            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              WithClose(cx.createStatement()) { st =>
                WithClose(st.executeQuery(sql)) { rs =>
                  rs.next() must beTrue
                  val tableType = rs.getString("table_type")
                  logger.debug(s"Table ${rs.getString("table_name")} is $tableType")
                  tableType mustEqual "unlogged"
                }
              }
            }
          }
        }
      }
    }

    "create child partitions with the configured toast target and covering dtg index" in {
      WithClose(datastore()) { ds =>
        foreach(Seq(false, true)) { covering =>
          val opts =
            s"${SftUserData.ToastTupleTarget.key}=512,${SftUserData.VisEnabled.key}=true,${SftUserData.CoveringDtgIndex.key}=$covering"
          testSchema(ds, s"child_storage_$covering", s"$spec,$opts") { sft =>
            SftUserData.ToastTupleTarget.get(sft) must beSome(512)
            SftUserData.CoveringDtgIndex.get(sft) mustEqual covering
            // the schema filters on visibility, which every written feature has to carry
            val features = this.features(sft).map(SecurityUtils.setFeatureVisibility(_, "user"))
            WithClose(new DefaultTransaction()) { tx =>
              WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
                features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              }
              tx.commit()
            }

            WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
              val typeInfo = TypeInfo(this.schema, sft)
              WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
              WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())

              // the features span 20 to 200 minutes ago, so there are both main and write ahead partitions
              def children(parent: TableConfig): Seq[(String, String)] = {
                val sql =
                  s"""SELECT c.relname, array_to_string(c.reloptions, ',')
                     |  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                     |  WHERE i.inhparent = ${parent.name.asRegclass}""".stripMargin
                WithClose(cx.createStatement().executeQuery(sql)) { rs =>
                  val result = ArrayBuffer.empty[(String, String)]
                  while (rs.next()) {
                    result += ((rs.getString(1), Option(rs.getString(2)).getOrElse("")))
                  }
                  result.toSeq
                }
              }

              val writeAhead = children(typeInfo.tables.writeAheadPartitions)
              writeAhead must not(beEmpty)
              foreach(writeAhead) { case (_, opts) => opts must not(contain("toast_tuple_target")) }
              val partitioned = children(typeInfo.tables.mainPartitions)
              partitioned must not(beEmpty)

              // main partitions are also created by merging the write ahead partitions once they age past the
              // cutoff, which is how most of them are made - run it as if a few hours had passed
              WithClose(cx.prepareCall(s"call ${MergeWriteAheadPartitions.name(typeInfo).quoted}(now()::timestamp + interval '3 hours');"))(_.execute())
              WithClose(cx.createStatement()) { st =>
                st.execute("SELECT set_config('geomesa.auths', 'user', false)")
                WithClose(st.executeQuery(s"SELECT count(*) FROM ${typeInfo.tables.view.name.qualified}")) { rs =>
                  rs.next() must beTrue
                  rs.getInt(1) mustEqual features.length
                }
              }
              val main = children(typeInfo.tables.mainPartitions)
              main.map(_._1) must containAllOf(partitioned.map(_._1))
              main.length must beGreaterThan(partitioned.length)
              foreach(main) { case (_, opts) => opts must contain("toast_tuple_target=512") }

              val expectedIndex = if (covering) {
                "(dtg, _vis) INCLUDE (geom)"
              } else {
                "(dtg, _vis)"
              }

              def checkIndex(partition: String): MatchResult[_] = {
                val sql =
                  s"SELECT indexdef FROM pg_indexes WHERE schemaname = '${this.schema}' AND tablename = '$partition'" +
                    s" AND indexdef LIKE '%USING btree (dtg%';"
                WithClose(cx.createStatement().executeQuery(sql)) { rs =>
                  rs.next() must beTrue
                  rs.getString(1) must endWith(expectedIndex)
                }
              }

              foreach(main.map(_._1))(checkIndex)

              // rebuilding a partition must keep the date/visibility keys and optional geometry payload
              val compactDate = new java.sql.Timestamp(features.last.getAttribute("dtg").asInstanceOf[java.util.Date].getTime)
              WithClose(cx.prepareCall(s"call ${CompactPartitions.name(typeInfo).quoted}(?);")) { st =>
                st.setTimestamp(1, compactDate)
                st.execute()
              }
              count(cx, typeInfo.tables.mainPartitions) mustEqual features.length
              foreach(children(typeInfo.tables.mainPartitions)) { case (partition, opts) =>
                opts must contain("toast_tuple_target=512")
                checkIndex(partition)
              }

              // visibility keys and included geometry aren't independently indexed attributes
              def parentIndices: Seq[String] = {
                val sql = s"SELECT indexdef FROM pg_indexes WHERE schemaname = '${this.schema}'" +
                  s" AND tablename = '${typeInfo.tables.mainPartitions.name.raw}' ORDER BY 1"
                WithClose(cx.createStatement().executeQuery(sql)) { rs =>
                  Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
                }
              }

              val before = parentIndices
              before must contain(endWith(expectedIndex))
              ds.getSchema(sft.getTypeName).getDescriptor(PartitionedPostgisDialect.VisCol) must beNull // user-facing
              ds.upgrade(ds.getSchema(sft.getTypeName))
              parentIndices mustEqual before
            }
          }
        }
      }
    }
  }

  def isTableLoggedQuery(tableName: String, schemaName: String): String = {
    s"""
       |SELECT
       |    n.nspname AS schema_name,
       |    c.relname AS table_name,
       |    CASE c.relpersistence
       |        WHEN 'u' THEN 'unlogged'
       |        WHEN 'p' THEN 'permanent'
       |        WHEN 't' THEN 'temporary'
       |        ELSE 'unknown'
       |    END AS table_type
       |FROM
       |    pg_class c
       |JOIN
       |    pg_namespace n ON n.oid = c.relnamespace
       |WHERE
       |    c.relname = '$tableName'
       |    AND n.nspname = '$schemaName';
       |""".stripMargin
  }
}
