/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect._
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{SequenceTable, UserDataTable}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.WithClose

import java.util.concurrent._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Try

class PartitionedPostgisDataStorePartitioningTest extends TestWithSharedPostgisContainer {

  "PartitionedPostgisDataStore" should {

    "route mutations to visible rows while write-ahead detachment is pending" in {
      WithClose(datastore()) { ds =>
        foreach(Seq("main", "spill")) { destination =>
          foreach(Seq(false, true)) { migrated =>
            testSchema(ds, s"mutations_${destination}_$migrated") { sft =>
              val info = TypeInfo(this.schema, sft)
              val source = TableIdentifier(this.schema, s"${info.typeIdentifier}_wa_test").qualified
              val target = TableIdentifier(this.schema, s"${info.typeIdentifier}_target_test").qualified
              val expired = TableIdentifier(this.schema, s"${info.typeIdentifier}_expired_test").qualified
              val parent = if (destination == "main") {
                info.tables.mainPartitions
              } else {
                info.tables.spillPartitions
              }
              val bounds = "FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2020-01-01 01:00:00')"
              val fid = info.cols.fid.quoted
              val dtg = info.cols.dtg.quoted
              WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
                WithClose(cx.createStatement()) { st =>
                  // Keep maintenance from changing this deliberately paused, post-merge state.
                  st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
                  st.execute(s"CREATE TABLE $source PARTITION OF ${info.tables.writeAheadPartitions.name.qualified} $bounds")
                  st.execute(s"INSERT INTO $source ($fid, age, $dtg) VALUES " +
                    "('update', 1, '2020-01-01 00:30:00'), ('delete', 2, '2020-01-01 00:30:00'), " +
                    "('keep', 3, '2020-01-01 00:30:00')")
                  if (migrated) {
                    st.execute(s"CREATE TABLE $target PARTITION OF ${parent.name.qualified} $bounds")
                    st.execute(s"INSERT INTO $target SELECT * FROM $source")
                    if (destination == "spill") {
                      // An expired main copy must not intercept mutations of the visible spill copy.
                      st.execute(s"CREATE TABLE $expired PARTITION OF ${info.tables.mainPartitions.name.qualified} $bounds")
                      st.execute(s"INSERT INTO $expired SELECT * FROM $source")
                      st.execute(s"INSERT INTO ${info.tables.ageOffQueue.name.qualified} VALUES " +
                        s"('$expired', '$expired'::regclass, ${info.tables.mainPartitions.name.asRegclass}, now())")
                    }
                    st.execute(s"INSERT INTO ${info.tables.writeAheadMigrations.name.qualified} " +
                      s"VALUES ('$source', '2020-01-01 00:00:00', '2020-01-01 01:00:00', now())")
                  }

                  def rows(table: String): Seq[(String, Int)] = {
                    WithClose(st.executeQuery(s"SELECT $fid, age FROM $table ORDER BY $fid")) { rs =>
                      val result = ArrayBuffer.empty[(String, Int)]
                      while (rs.next()) {
                        result += rs.getString(1) -> rs.getInt(2)
                      }
                      result.toSeq
                    }
                  }

                  val view = info.tables.view.name.qualified
                  rows(view) mustEqual Seq("delete" -> 2, "keep" -> 3, "update" -> 1)
                  // Move outside the migrated interval: routing must use OLD.dtg.
                  st.executeUpdate(s"UPDATE $view SET age = 10, $dtg = '2020-01-01 02:30:00' WHERE $fid = 'update'") mustEqual 1
                  // The updated row now lives in the current write-ahead table.
                  st.executeUpdate(s"UPDATE $view SET age = 11 WHERE $fid = 'update'") mustEqual 1
                  st.executeUpdate(s"DELETE FROM $view WHERE $fid = 'delete'") mustEqual 1
                  rows(view) mustEqual Seq("keep" -> 3, "update" -> 11)

                  if (migrated) {
                    rows(source) mustEqual Seq("delete" -> 2, "keep" -> 3, "update" -> 1)
                    rows(target) mustEqual Seq("keep" -> 3)
                    if (destination == "spill") {
                      rows(expired) mustEqual Seq("delete" -> 2, "keep" -> 3, "update" -> 1)
                    }
                    st.execute(s"ALTER TABLE ${info.tables.writeAheadPartitions.name.qualified} " +
                      s"DETACH PARTITION $source CONCURRENTLY")
                    st.execute(s"CALL ${info.schema.quoted}.${CleanWriteAheadMigrations.name(info).quoted}()")
                    rows(view) mustEqual Seq("keep" -> 3, "update" -> 11)
                    WithClose(st.executeQuery(s"SELECT to_regclass('$source')")) { rs =>
                      rs.next() must beTrue
                      rs.getString(1) must beNull
                    }
                    WithClose(st.executeQuery(s"SELECT count(*) FROM ${info.tables.writeAheadMigrations.name.qualified}")) { rs =>
                      rs.next() must beTrue
                      rs.getInt(1) mustEqual 0
                    }
                  } else {
                    rows(source) mustEqual Seq("keep" -> 3)
                  }
                }
              }
            }
          }
        }
      }
    }

    "keep maintenance advancing while retired numeric write-ahead tables are still being read" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "numeric_wa_locks", s"$spec,${SftUserData.MaxPartitions.key}=2") { sft =>
          val info = TypeInfo(this.schema, sft)
          val source = TableIdentifier(this.schema, info.tables.writeAhead.name.raw + "_000")
          val later = TableIdentifier(this.schema, info.tables.writeAhead.name.raw + "_001")
          val newPartition = TableIdentifier(this.schema, info.tables.mainPartitions.name.raw + "_2020_01_01_01")
          val invocation = s"CALL ${info.schema.quoted}.${PartitionWriteAheadLog.name(info).quoted}('2020-01-01 05:00:00')"
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { observer =>
            WithClose(observer.createStatement()) { st =>
              def scalar(sql: String): Int = WithClose(st.executeQuery(sql)) { rs => rs.next(); rs.getInt(1) }

              def exists(table: TableIdentifier): Boolean =
                scalar(s"SELECT count(*) FROM pg_class WHERE oid = to_regclass('${table.qualified}')") == 1

              def pauseJobs(): Unit =
                st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")

              st.execute("SET statement_timeout = '5s'")
              pauseJobs()
              Seq(info.tables.mainPartitions, info.tables.spillPartitions).foreach { parent =>
                val child = TableIdentifier(this.schema, parent.name.raw + "_2020_01_01_00")
                st.execute(s"CREATE TABLE ${child.qualified} PARTITION OF ${parent.name.qualified} " +
                  "FOR VALUES FROM ('2020-01-01') TO ('2020-01-01 01:00:00')")
                st.execute(s"INSERT INTO ${child.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}, geom) " +
                  s"VALUES ('${parent.name.raw}', '2020-01-01 00:30:00', ST_GeomFromText('POINT(0 0)', 4326))")
              }
              val view = info.tables.view.name.qualified
              st.execute(s"INSERT INTO $view (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}, geom) VALUES " +
                "('copy-to-spill', '2020-01-01 00:30:00', ST_GeomFromText('POINT(0 0)', 4326)), " +
                "('copy-to-main', '2020-01-01 01:30:00', ST_GeomFromText('POINT(0 0)', 4326))")
              st.execute(s"CALL ${info.schema.quoted}.${RollWriteAheadLog.name(info).quoted}()")
              // Seed a timed partition so the later maintenance call must also run merging.
              val timed = TableIdentifier(this.schema, info.tables.writeAheadPartitions.name.raw + "_2020_01_01_03_00")
              st.execute(s"CREATE TABLE ${timed.qualified} PARTITION OF ${info.tables.writeAheadPartitions.name.qualified} " +
                "FOR VALUES FROM ('2020-01-01 03:00:00') TO ('2020-01-01 04:00:00')")
              st.execute(s"INSERT INTO ${timed.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}, geom) VALUES " +
                "('merge-me', '2020-01-01 03:30:00', ST_GeomFromText('POINT(0 0)', 4326))")

              WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { reader =>
                reader.setAutoCommit(false)
                try {
                  WithClose(reader.createStatement()) { read =>
                    WithClose(read.executeQuery(s"SELECT count(*) FROM $view")) { rs =>
                      rs.next() must beTrue
                      rs.getInt(1) mustEqual 5
                    }
                  }
                  // The export keeps its relation locks throughout every maintenance attempt below.
                  st.execute(invocation)
                  scalar(s"SELECT count(*) FROM $view") mustEqual 5
                  exists(source) must beTrue
                  exists(newPartition) must beTrue
                  scalar(s"SELECT count(*) FROM ${info.tables.writeAheadRetirements.name.qualified}") mustEqual 1
                  scalar(s"SELECT count(*) FROM ${info.tables.spillPartitions.name.qualified}") mustEqual 2
                  scalar(s"SELECT count(*) FROM ${info.tables.mainPartitions.name.qualified}") mustEqual 2

                  st.executeUpdate(s"UPDATE $view SET age = 11 WHERE ${info.cols.fid.quoted} = 'copy-to-spill'") mustEqual 1
                  st.executeUpdate(s"DELETE FROM $view WHERE ${info.cols.fid.quoted} = 'copy-to-main'") mustEqual 1
                  scalar(s"SELECT count(*) FROM $view") mustEqual 4
                  st.execute(s"INSERT INTO $view (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}, geom) VALUES " +
                    "('later-row', '2020-01-01 02:30:00', ST_GeomFromText('POINT(0 0)', 4326))")
                  st.execute(s"CALL ${info.schema.quoted}.${RollWriteAheadLog.name(info).quoted}()")
                  val maintenance = s"CALL ${info.schema.quoted}.${PartitionMaintenance.name(info).quoted}()"
                  (0 until 3).foreach { _ =>
                    st.execute(maintenance)
                    pauseJobs()
                    scalar(s"SELECT count(*) FROM ${info.tables.writeAheadRetirements.name.qualified}") mustEqual 2
                    scalar(s"SELECT count(*) FROM ${info.tables.mainPartitions.name.qualified}") mustEqual 3
                    scalar(s"SELECT count(*) FROM ${info.tables.spillPartitions.name.qualified}") mustEqual 2
                    scalar(s"SELECT count(*) FROM ${info.tables.writeAheadMigrations.name.qualified}") mustEqual 1
                    scalar(s"SELECT count(*) FROM ${info.tables.ageOffQueue.name.qualified}") mustEqual 5
                    scalar(s"SELECT count(*) FROM $view") mustEqual 0
                  }
                  exists(source) must beTrue
                  exists(later) must beTrue
                  st.execute(s"INSERT INTO $view (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}, geom) VALUES " +
                    "('cleanup-free', '2020-01-01 04:30:00', ST_GeomFromText('POINT(0 0)', 4326))")
                  st.execute(s"CALL ${info.schema.quoted}.${RollWriteAheadLog.name(info).quoted}()")
                  st.execute(maintenance)
                  pauseJobs()
                  scalar(s"SELECT count(*) FROM ${info.tables.writeAheadRetirements.name.qualified}") mustEqual 3
                  scalar(s"SELECT count(*) FROM ${info.tables.mainPartitions.name.qualified}") mustEqual 4
                  scalar(s"SELECT count(*) FROM $view") mustEqual 0
                  reader.rollback()
                  WithClose(reader.createStatement()) { read =>
                    WithClose(read.executeQuery(s"SELECT count(*) FROM ONLY ${info.tables.writeAhead.name.qualified}")) { rs =>
                      rs.next() must beTrue
                      rs.getInt(1) mustEqual 0
                    }
                  }
                  // proc will skip detach due to open reader
                  st.execute(s"CALL ${info.schema.quoted}.${CleanWriteAheadRetirements.name(info).quoted}()")
                  exists(source) must beTrue
                  exists(later) must beTrue
                  scalar(s"SELECT count(*) FROM ${info.tables.writeAheadRetirements.name.qualified}") mustEqual 3
                  reader.rollback()
                  // now proc will detach and drop tables
                  st.execute(s"CALL ${info.schema.quoted}.${CleanWriteAheadRetirements.name(info).quoted}()")
                  exists(source) must beFalse
                  exists(later) must beFalse
                  scalar(s"SELECT count(*) FROM ${info.tables.writeAheadRetirements.name.qualified}") mustEqual 0
                } finally {
                  reader.rollback()
                }
              }
            }
          }
        }
      }
    }

    "recover interrupted timed write-ahead detaches and advance the next queued partition" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "timed_detach_retry") { sft =>
          val info = TypeInfo(this.schema, sft)
          val sources = (0 until 2).map(hour => TableIdentifier(this.schema,
            info.tables.writeAheadPartitions.name.raw + f"_2020_01_01_$hour%02d" + "_00"))
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { observer =>
            WithClose(observer.createStatement()) { st =>
              def scalar(sql: String): Int = WithClose(st.executeQuery(sql)) { rs => rs.next(); rs.getInt(1) }

              def pauseJobs(): Unit = {
                st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              }

              def clean(): Unit = {
                st.execute(s"CALL ${info.schema.quoted}.${CleanWriteAheadMigrations.name(info).quoted}()")
                pauseJobs()
              }

              def command(): String = WithClose(st.executeQuery(s"SELECT command FROM cron.job WHERE jobname = " +
                CleanWriteAheadMigrations.detachJobName(info).quoted)) { rs =>
                rs.next() must beTrue
                rs.getString(1)
              }

              def workers(): Int = scalar(s"SELECT count(*) FROM cron.job WHERE jobname LIKE '${info.typeIdentifier}-detach-%'")

              st.execute("SET statement_timeout = '5s'")
              pauseJobs()
              sources.zipWithIndex.foreach { case (source, hour) =>
                val target = TableIdentifier(this.schema, info.tables.mainPartitions.name.raw + f"_2020_01_01_$hour%02d")
                val bounds = s"FOR VALUES FROM ('2020-01-01 $hour:00:00') TO ('2020-01-01 ${hour + 1}:00:00')"
                st.execute(s"CREATE TABLE ${source.qualified} PARTITION OF ${info.tables.writeAheadPartitions.name.qualified} $bounds")
                st.execute(s"INSERT INTO ${source.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) " +
                  s"VALUES ('fid$hour', '2020-01-01 $hour:30:00')")
                st.execute(s"CREATE TABLE ${target.qualified} PARTITION OF ${info.tables.mainPartitions.name.qualified} $bounds")
                st.execute(s"INSERT INTO ${target.qualified} SELECT * FROM ${source.qualified}")
                st.execute(s"INSERT INTO ${info.tables.writeAheadMigrations.name.qualified} VALUES " +
                  s"('${source.qualified}', '2020-01-01 $hour:00:00', '2020-01-01 ${hour + 1}:00:00', now())")
                // Simulate jobs left by the original per-partition scheduling scheme.
                st.execute(s"SELECT cron.schedule('${info.typeIdentifier}-detach-' || split_part('${source.qualified}', '.', 2), " +
                  "'0 0 1 1 *', 'SELECT 1')")
              }
              clean()
              workers() mustEqual 1
              command() must endWith(" CONCURRENTLY")
              command() must contain(sources.head.qualified)
              val view = info.tables.view.name.qualified
              WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { reader =>
                reader.setAutoCommit(false)
                try {
                  WithClose(reader.createStatement()) { read =>
                    WithClose(read.executeQuery(s"SELECT count(*) FROM $view")) { rs =>
                      rs.next() must beTrue
                      rs.getInt(1) mustEqual 2
                    }
                  }
                  WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { worker =>
                    val pid = WithClose(worker.createStatement()) { query =>
                      WithClose(query.executeQuery("SELECT pg_backend_pid()")) { rs => rs.next(); rs.getInt(1) }
                    }
                    WithClose(worker.createStatement()) { detach =>
                      detach.setQueryTimeout(30)
                      val sql = command()
                      val task = CachedThreadPool.call(() => detach.execute(sql))
                      try {
                        eventually(50, 100.millis) {
                          scalar(s"SELECT count(*) FROM pg_stat_activity WHERE pid = $pid " +
                            "AND wait_event_type = 'Lock' AND wait_event = 'virtualxid'") mustEqual 1
                        }
                        scalar(s"SELECT count(*) FROM $view") mustEqual 2
                        detach.cancel()
                        Try(task.get(5, TimeUnit.SECONDS)).isFailure must beTrue
                        clean()
                        workers() mustEqual 1
                        command() must endWith(" FINALIZE")
                        command() must contain(sources.head.qualified)
                        // Merge excludes these ranges; recurring recovery must still run without recopying them.
                        st.execute(s"CALL ${info.schema.quoted}.${MergeWriteAheadPartitions.name(info).quoted}('2020-01-01 05:00:00')")
                        pauseJobs()
                        command() must endWith(" FINALIZE")
                        scalar(s"SELECT count(*) FROM ${info.tables.mainPartitions.name.qualified}") mustEqual 2
                      } finally {
                        reader.rollback()
                        if (!task.isDone) {
                          detach.cancel()
                          Try(task.get(5, TimeUnit.SECONDS))
                        }
                      }
                    }
                  }
                } finally {
                  reader.rollback()
                }
              }
              // note: this can cause occasional test failures due to deadlocking with a scheduled cron run
              st.execute(command()) // FINALIZE the interrupted first detach.
              clean()
              scalar(s"SELECT count(*) FROM ${info.tables.writeAheadMigrations.name.qualified}") mustEqual 1
              workers() mustEqual 1
              command() must endWith(" CONCURRENTLY")
              command() must contain(sources.last.qualified)
              st.execute(command())
              clean()
              workers() mustEqual 0
              scalar(s"SELECT count(*) FROM ${info.tables.writeAheadMigrations.name.qualified}") mustEqual 0
              foreach(sources) { source =>
                scalar(s"SELECT count(*) FROM pg_class WHERE oid = to_regclass('${source.qualified}')") mustEqual 0
              }
              scalar(s"SELECT count(*) FROM $view") mustEqual 2
            }
          }
        }
      }
    }

    "skip unmigrated numeric write-ahead slots when rotating through zero" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "wa_rotation") { sft =>
          val info = TypeInfo(this.schema, sft)
          val occupied = TableIdentifier(this.schema, info.tables.writeAhead.name.raw + "_000")
          val current = TableIdentifier(this.schema, info.tables.writeAhead.name.raw + "_999")
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            WithClose(cx.createStatement()) { st =>
              st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              st.execute(s"INSERT INTO ${occupied.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) " +
                "VALUES ('unmigrated', '2020-01-01')")
              st.execute(s"CREATE TABLE ${current.qualified} () INHERITS (${info.tables.writeAhead.name.qualified})")
              st.execute(s"INSERT INTO ${current.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) " +
                "VALUES ('active', '2020-01-02')")
              st.execute(s"UPDATE ${info.schema.quoted}.${SequenceTable.Name.quoted} SET value = 999 " +
                s"WHERE type_name = '${sft.getTypeName}'")
              st.execute(s"CALL ${info.schema.quoted}.${RollWriteAheadLog.name(info).quoted}()")
              WithClose(st.executeQuery(s"SELECT value FROM ${info.schema.quoted}.${SequenceTable.Name.quoted} " +
                s"WHERE type_name = '${sft.getTypeName}'")) { rs =>
                rs.next() must beTrue
                rs.getInt(1) mustEqual 1
              }
              st.execute(s"INSERT INTO ${info.tables.view.name.qualified} (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) " +
                "VALUES ('new', '2020-01-03')")
              WithClose(st.executeQuery(s"SELECT count(*) FROM ${occupied.qualified}")) { rs =>
                rs.next() must beTrue
                rs.getInt(1) mustEqual 1
              }
              WithClose(st.executeQuery(s"SELECT count(*) FROM ${info.tables.view.name.qualified}")) { rs =>
                rs.next() must beTrue
                rs.getInt(1) mustEqual 3
              }
            }
          }
        }
      }
    }

    "age off without blocking new queries and recover an interrupted detach" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "concurrent_ageoff", s"$spec,${SftUserData.MaxPartitions.key}=2") { sft =>
          val info = TypeInfo(this.schema, sft)
          val parents = Seq(info.tables.mainPartitions, info.tables.spillPartitions)
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { observer =>
            WithClose(observer.createStatement()) { st =>
              def scalar(sql: String): Int = WithClose(st.executeQuery(sql)) { rs => rs.next(); rs.getInt(1) }

              def clean(): Unit = {
                st.execute(s"CALL ${info.schema.quoted}.${CleanAgedOffPartitions.name(info).quoted}()")
                // Invoke worker commands ourselves so the test does not depend on cron timing.
                st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              }

              def command(parent: TableConfig): String = {
                WithClose(st.executeQuery(s"SELECT command FROM cron.job WHERE jobname = " +
                  CleanAgedOffPartitions.detachJobName(info, parent).quoted)) { rs =>
                  rs.next() must beTrue
                  rs.getString(1)
                }
              }

              st.execute("SET statement_timeout = '5s'")
              st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              parents.foreach { parent =>
                Seq(0, 1, 3).foreach { hour =>
                  val child = TableIdentifier(this.schema, parent.name.raw + f"_2020_01_01_$hour%02d").qualified
                  st.execute(s"CREATE TABLE $child PARTITION OF ${parent.name.qualified} " +
                    s"FOR VALUES FROM ('2020-01-01 $hour:00:00') TO ('2020-01-01 ${hour + 1}:00:00')")
                  st.execute(s"INSERT INTO $child (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) " +
                    s"VALUES ('${parent.name.raw}_$hour', '2020-01-01 $hour:30:00')")
                }
              }
              val view = info.tables.view.name.qualified
              WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { reader =>
                reader.setAutoCommit(false)
                try {
                  // Exhausting the result still holds relation locks until the export transaction ends.
                  WithClose(reader.createStatement()) { read =>
                    WithClose(read.executeQuery(s"SELECT count(*) FROM $view")) { rs =>
                      rs.next() must beTrue
                      rs.getInt(1) mustEqual 6
                    }
                  }
                  st.execute(s"CALL ${info.schema.quoted}.${DropAgedOffPartitions.name(info).quoted}" +
                    "('2020-01-01 05:00:00')")
                  clean()
                  scalar(s"SELECT count(*) FROM ${info.tables.ageOffQueue.name.qualified}") mustEqual 4
                  scalar(s"SELECT count(*) FROM $view") mustEqual 2

                  WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { worker =>
                    val pid = WithClose(worker.createStatement()) { query =>
                      WithClose(query.executeQuery("SELECT pg_backend_pid()")) { rs => rs.next(); rs.getInt(1) }
                    }
                    WithClose(worker.createStatement()) { detach =>
                      detach.setQueryTimeout(30)
                      val sql = command(parents.head)
                      sql must endWith(" CONCURRENTLY")
                      val task = CachedThreadPool.call(() => detach.execute(sql))
                      try {
                        eventually(50, 100.millis) {
                          scalar(s"SELECT count(*) FROM pg_stat_activity WHERE pid = $pid " +
                            "AND wait_event_type = 'Lock' AND wait_event = 'virtualxid'") mustEqual 1
                        }
                        task.isDone must beFalse
                        // This query must finish while the old export keeps detachment waiting.
                        scalar(s"SELECT count(*) FROM $view") mustEqual 2
                        detach.cancel()
                        Try(task.get(5, TimeUnit.SECONDS)).isFailure must beTrue
                        scalar(s"SELECT count(*) FROM pg_inherits WHERE inhparent = " +
                          s"${parents.head.name.asRegclass} AND inhdetachpending") mustEqual 1
                        clean()
                        command(parents.head) must endWith(" FINALIZE")
                      } finally {
                        reader.rollback()
                        if (!task.isDone) {
                          detach.cancel()
                          Try(task.get(5, TimeUnit.SECONDS))
                        }
                      }
                    }
                  }
                } finally {
                  reader.rollback()
                }
              }

              foreach(parents) { parent =>
                (0 until 2).foreach { _ =>
                  st.execute(command(parent))
                  clean()
                }
                scalar(s"SELECT count(*) FROM cron.job WHERE jobname = " +
                  CleanAgedOffPartitions.detachJobName(info, parent).quoted) mustEqual 0
                scalar(s"SELECT count(*) FROM ${parent.name.qualified}") mustEqual 1
                scalar(s"SELECT count(*) FROM pg_class WHERE relname IN " +
                  s"('${parent.name.raw}_2020_01_01_00', '${parent.name.raw}_2020_01_01_01')") mustEqual 0
              }
              scalar(s"SELECT count(*) FROM ${info.tables.ageOffQueue.name.qualified}") mustEqual 0
              scalar(s"SELECT count(*) FROM $view") mustEqual 2
            }
          }
        }
      }
    }

    "remove queued age-off partitions and worker jobs with the schema" in {
      WithClose(datastore()) { ds =>
        WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
          var info: TypeInfo = null
          var children: Seq[TableIdentifier] = null
          WithClose(cx.createStatement()) { st =>
            testSchema(ds, "age-off-removal", s"$spec,${SftUserData.MaxPartitions.key}=2") { sft =>
              info = TypeInfo(this.schema, sft)
              val parents = Seq(info.tables.mainPartitions, info.tables.spillPartitions)
              children = parents.map(parent => TableIdentifier(this.schema, parent.name.raw + "_2020_01_01_00"))

              st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              parents.zip(children).foreach { case (parent, child) =>
                st.execute(s"CREATE TABLE ${child.qualified} PARTITION OF ${parent.name.qualified} " +
                  "FOR VALUES FROM ('2020-01-01') TO ('2020-01-01 01:00:00')")
              }
              st.execute(s"CALL ${info.schema.quoted}.${DropAgedOffPartitions.name(info).quoted}" +
                "('2020-01-01 05:00:00')")
              st.execute(s"UPDATE cron.job SET active = false WHERE jobname LIKE '${info.typeIdentifier}-%'")
              val command = WithClose(st.executeQuery(s"SELECT command FROM cron.job WHERE jobname = " +
                CleanAgedOffPartitions.detachJobName(info, parents.head).quoted)) { rs =>
                rs.next() must beTrue
                rs.getString(1)
              }
              // Leave one detached partition and one attached partition in the queue.
              st.execute(command)
            }
            foreach(children) { child =>
              WithClose(st.executeQuery(s"SELECT to_regclass('${child.qualified}')")) { rs =>
                rs.next() must beTrue
                rs.getString(1) must beNull
              }
            }
            WithClose(st.executeQuery(s"SELECT count(*) FROM cron.job WHERE jobname LIKE '${info.typeIdentifier}-%'")) { rs =>
              rs.next() must beTrue
              rs.getInt(1) mustEqual 0
            }
          }
        }
      }
    }

    "age-off" in {
      WithClose(datastore()) { ds =>
        testSchema(ds, "age-off", s"$spec,${SftUserData.MaxPartitions.key}=2") { sft =>
          writeFeatures(ds, sft)
          // verify data is being partitioned as expected
          WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val typeInfo = TypeInfo(this.schema, sft)
            // initially everything is in the write ahead log
            foreach(Seq(typeInfo.tables.view, typeInfo.tables.writeAhead))(table => count(cx, table) mustEqual 10)
            foreach(Seq(typeInfo.tables.writeAheadPartitions, typeInfo.tables.mainPartitions)) { table =>
              count(cx, table) mustEqual 0
            }
            // manually invoke the scheduled crons so we don't have to wait
            WithClose(cx.prepareCall(s"call ${RollWriteAheadLog.name(typeInfo).quoted}();"))(_.execute())
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
            // verify that data was aged off appropriately - exact age-off depends on time test was run
            count(cx, typeInfo.tables.view) must beOneOf(6, 7, 8)
          }
        }
      }
    }

    "support partition size change through user_data table" in {
      val spec = this.spec.replace(s"${SftUserData.IntervalHours.key}=$hours", s"${SftUserData.IntervalHours.key}=6")
      WithClose(datastore()) { ds =>
        testSchema(ds, "partition-size", spec) { sft =>
          writeFeatures(ds, sft)

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
            // initially main partitions will not have any data due to 6 hour partition size
            count(cx, typeInfo.tables.mainPartitions) mustEqual 0
            // update the partition count
            WithClose(cx.createStatement()) { st =>
              val sql =
                s"update ${UserDataTable.Name.quoted} set value = '1' where type_name = '${sft.getTypeName}' and " +
                  s"key = '${SftUserData.IntervalHours.key}'"
              st.executeUpdate(sql) mustEqual 1
            }
            // re-run the scheduled crons
            WithClose(cx.prepareCall(s"call ${PartitionMaintenance.name(typeInfo).quoted}();"))(_.execute())
            // verify that data was sorted into the appropriate tables based on dtg
            count(cx, typeInfo.tables.view) mustEqual 10
            count(cx, typeInfo.tables.writeAhead) mustEqual 0
            count(cx, typeInfo.tables.writeAheadPartitions) must beGreaterThan(0)
            // now main partitions should have some data due to 1 hour partition size
            count(cx, typeInfo.tables.mainPartitions) must beGreaterThan(0)
          }
        }
      }
    }
  }
}
