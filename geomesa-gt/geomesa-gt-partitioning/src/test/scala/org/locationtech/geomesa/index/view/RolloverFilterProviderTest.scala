/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.gt.partition.postgis.TestWithSharedPostgisContainer
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.index.view.filters.RolloverFilterProvider
import org.locationtech.geomesa.index.view.filters.RolloverFilterProvider.{CoverageInterval, JdbcRolloverProvider}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.sql.{Connection, DriverManager}
import java.time.{Instant, ZoneOffset}
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

class RolloverFilterProviderTest extends SpecificationWithJUnit {

  sequential

  private val now = Instant.parse("2026-01-02T00:00:00Z")
  private val start = now.minusSeconds(3600)

  class Provider extends JdbcRolloverProvider {
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = None
    def lookup(typeName: String, context: QueryContext, dtg: String = "dtg"): Option[CoverageInterval] =
      coverage(SimpleFeatureTypes.createType(typeName, s"$dtg:Date,*geom:Point:srid=4326"), context)
  }

  private def fixture(f: (Connection, String, Provider) => Any): Unit = {
    val container = TestWithSharedPostgisContainer.container
    val url = s"jdbc:postgresql://${container.getHost}:${container.getFirstMappedPort}/postgres"
    val table = "Rollover_" + UUID.randomUUID().toString.replace("-", "")
    WithClose(DriverManager.getConnection(url, "postgres", container.password)) { cx =>
      WithClose(cx.createStatement())(_.execute("SET TIME ZONE 'Pacific/Honolulu'"))
      RolloverFilterProvider.createTable(cx, s"public.$table")
      val provider = new Provider
      try {
        provider.init(Map("schema" -> "public", "table" -> table, "url" -> url,
          "username" -> "postgres", "password" -> container.password, "max-connections" -> "1"))
        f(cx, table, provider)
      } finally {
        try { provider.close() } finally {
          WithClose(cx.createStatement())(_.execute(s"""DROP TABLE public."$table""""))
        }
      }
    }
  }

  private def insert(cx: Connection, table: String, name: String, to: Instant): Unit =
    RolloverFilterProvider.insertCoverage(cx, s"public.$table", CoverageInterval(name, start, to))

  private def count(cx: Connection, table: String): Long = {
    WithClose(cx.createStatement()) { statement =>
      WithClose(statement.executeQuery(s"""SELECT count(*) FROM public."$table"""")) { results =>
        results.next() must beTrue
        results.getLong(1)
      }
    }
  }

  "JDBC rollover coverage" should {
    "isolate query contexts with identical timestamps and reject mixed coverage relations" in {
      fixture { (cx, table, provider) =>
        fixture { (otherCx, otherTable, otherProvider) =>
          insert(cx, table, "shared", now)
          insert(otherCx, otherTable, "shared", now.plusSeconds(60))
          val context = QueryContext(Filter.INCLUDE, now)
          provider.lookup("shared", context).get.through mustEqual now
          otherProvider.lookup("shared", context) must throwA[IllegalArgumentException](".*different tables.*")
          otherProvider.lookup("shared", QueryContext(Filter.INCLUDE, now)).get.through mustEqual now.plusSeconds(60)
          WithClose(cx.createStatement())(_.execute(s"""DELETE FROM public."$table""""))
          provider.lookup("shared", context).get.through mustEqual now
          provider.lookup("shared", QueryContext(Filter.INCLUDE, now)) must beNone
        }
      }
      success
    }

    "share a snapshot between providers for the same relation and validate their date fields" in {
      fixture { (cx, table, provider) =>
        val paired = new Provider
        try {
          paired.init(Map("schema" -> "public", "table" -> table, "url" -> cx.getMetaData.getURL,
            "username" -> "postgres", "password" -> TestWithSharedPostgisContainer.container.password))
          insert(cx, table, "paired", now)
          val context = QueryContext(Filter.INCLUDE, now)
          provider.lookup("paired", context) must beSome
          WithClose(cx.createStatement())(_.execute(s"""DELETE FROM public."$table""""))
          paired.lookup("paired", context) must beSome(CoverageInterval("paired", start, now))
          paired.lookup("paired", context, "otherDate") must
            throwA[IllegalArgumentException](".*different default date fields.*")
          paired.lookup("paired", QueryContext(Filter.INCLUDE, now)) must beNone
        } finally { paired.close() }
      }
      success
    }

    "retain an empty snapshot only for its query context" in {
      fixture { (cx, table, provider) =>
        val context = QueryContext(Filter.INCLUDE, now)
        provider.lookup("empty", context) must beNone
        insert(cx, table, "empty", now)
        provider.lookup("empty", context) must beNone
        provider.lookup("empty", QueryContext(Filter.INCLUDE, now)) must beSome
      }
      success
    }

    "interpret coverage bounds in UTC independently of the database session time zone" in {
      fixture { (cx, table, _) =>
        val provider = new Provider
        try {
          provider.init(Map("schema" -> "public", "table" -> table,
            "url" -> (cx.getMetaData.getURL + "?options=-c%20TimeZone%3DPacific%2FHonolulu"),
            "username" -> "postgres", "password" -> TestWithSharedPostgisContainer.container.password))
          insert(cx, table, "timezone", now)
          provider.lookup("timezone", QueryContext(Filter.INCLUDE, now)) must beSome(CoverageInterval("timezone", start, now))
        } finally { provider.close() }
      }
      success
    }

    "serialize concurrent replacements and retain one row per type" in {
      fixture { (cx, table, provider) =>
        insert(cx, table, "concurrent", now)
        cx.setAutoCommit(false)
        val executor = Executors.newSingleThreadExecutor()
        val started = new CountDownLatch(1)
        val finished = new CountDownLatch(1)
        try {
          insert(cx, table, "concurrent", now.plusSeconds(10))
          val future = executor.submit(new java.util.concurrent.Callable[Unit] {
            override def call(): Unit = {
              WithClose(DriverManager.getConnection(cx.getMetaData.getURL, "postgres",
                TestWithSharedPostgisContainer.container.password)) { other =>
                started.countDown()
                insert(other, table, "concurrent", now.plusSeconds(20))
                finished.countDown()
              }
            }
          })
          started.await(10, TimeUnit.SECONDS) must beTrue
          finished.await(200, TimeUnit.MILLISECONDS) must beFalse
          cx.commit()
          future.get(10, TimeUnit.SECONDS)
          count(cx, table) mustEqual 1L
          provider.lookup("concurrent", QueryContext(Filter.INCLUDE, now)) must
            beSome(CoverageInterval("concurrent", start, now.plusSeconds(20)))
        } finally {
          cx.rollback()
          cx.setAutoCommit(true)
          executor.shutdownNow()
        }
      }
      success
    }

    "create a table idempotently and insert intervals with caller-managed transactions" in {
      fixture { (cx, table, provider) =>
        val interval = CoverageInterval("helper's type", start, now)
        WithClose(cx.getMetaData.getColumns(null, "public", table, "%")) { columns =>
          val types = Vector.newBuilder[(String, String)]
          while (columns.next()) { types += columns.getString("COLUMN_NAME") -> columns.getString("TYPE_NAME") }
          types.result() mustEqual Vector("typename" -> "text", "covered_from" -> "timestamp", "covered_through" -> "timestamp")
        }
        RolloverFilterProvider.createTable(cx, table)
        cx.setAutoCommit(false)
        try {
          RolloverFilterProvider.insertCoverage(cx, table, interval)
          RolloverFilterProvider.createTable(cx, s"public.$table")
          count(cx, table) mustEqual 1L
          cx.isClosed must beFalse
          cx.getAutoCommit must beFalse
          cx.rollback()
          count(cx, table) mustEqual 0L
          RolloverFilterProvider.insertCoverage(cx, s"public.$table", interval)
          cx.commit()
          provider.lookup(interval.typeName, QueryContext(Filter.INCLUDE, now)) must beSome(interval)
          count(cx, table) mustEqual 1L
          cx.isClosed must beFalse
        } finally { cx.rollback(); cx.setAutoCommit(true) }
      }
      success
    }

    "replace both coverage bounds while preserving other types" in {
      fixture { (cx, table, provider) =>
        insert(cx, table, "other", now)
        insert(cx, table, "replacement", now)
        val interval = CoverageInterval("replacement", start.plusSeconds(60), now.plusSeconds(60))
        RolloverFilterProvider.insertCoverage(cx, s"public.$table", interval)
        count(cx, table) mustEqual 2L
        provider.lookup("replacement", QueryContext(Filter.INCLUDE, now)) must beSome(interval)
        provider.lookup("other", QueryContext(Filter.INCLUDE, now)) must beSome(CoverageInterval("other", start, now))
      }
      success
    }

    "roll back and commit replacements in the caller's transaction" in {
      fixture { (cx, table, provider) =>
        insert(cx, table, "transaction", now)
        cx.setAutoCommit(false)
        try {
          insert(cx, table, "transaction", now.plusSeconds(10))
          cx.isClosed must beFalse
          cx.getAutoCommit must beFalse
          provider.lookup("transaction", QueryContext(Filter.INCLUDE, now)) must
            beSome(CoverageInterval("transaction", start, now))
          cx.rollback()
          provider.lookup("transaction", QueryContext(Filter.INCLUDE, now)) must
            beSome(CoverageInterval("transaction", start, now))
          insert(cx, table, "transaction", now.plusSeconds(20))
          cx.commit()
          provider.lookup("transaction", QueryContext(Filter.INCLUDE, now)) must
            beSome(CoverageInterval("transaction", start, now.plusSeconds(20)))
          count(cx, table) mustEqual 1L
        } finally { cx.rollback(); cx.setAutoCommit(true) }
      }
      success
    }

    "reject invalid table names before executing SQL" in {
      fixture { (cx, table, _) =>
        foreach(Seq("", ".table", "public.", "public..table", "db.public.table", "table; DROP TABLE other", "table--")) { invalid =>
          RolloverFilterProvider.createTable(cx, invalid) must throwA[IllegalArgumentException]
          RolloverFilterProvider.insertCoverage(cx, invalid, CoverageInterval("helper", start, now)) must
            throwA[IllegalArgumentException]
        }
        count(cx, table) mustEqual 0L
        cx.isClosed must beFalse
      }
      success
    }

    "read current coverage independently of query time and retain it for the query context" in {
      fixture { (cx, table, provider) =>
        insert(cx, table, "track", now.minusSeconds(100))
        insert(cx, table, "other", now.plusSeconds(300))
        val context = QueryContext(Filter.INCLUDE, now)
        provider.lookup("track", context) must beSome(CoverageInterval("track", start, now.minusSeconds(100)))
        insert(cx, table, "track", now)
        provider.lookup("track", context) must beSome(CoverageInterval("track", start, now.minusSeconds(100)))
        provider.lookup("track", QueryContext(Filter.INCLUDE, now.minusSeconds(1))) must
          beSome(CoverageInterval("track", start, now))
        provider.lookup("other", QueryContext(Filter.INCLUDE, now)) must
          beSome(CoverageInterval("other", start, now.plusSeconds(300)))
        provider.lookup("missing", QueryContext(Filter.INCLUDE, now)) must beNone
      }
      success
    }

    "release pooled JDBC resources after successful, empty and failed lookups" in {
      fixture { (cx, table, provider) =>
        insert(cx, table, "valid", now)
        // Simulate malformed rows in a legacy table to exercise the reader's validation and cleanup.
        WithClose(cx.createStatement()) { statement =>
          statement.execute(s"""ALTER TABLE public."$table" ALTER COLUMN covered_from DROP NOT NULL,
            |ALTER COLUMN covered_through DROP NOT NULL""".stripMargin)
        }
        WithClose(cx.prepareStatement(
          s"""INSERT INTO public."$table" (typeName, covered_from, covered_through) VALUES (?, ?, ?)""")) { statement =>
          Seq("null-from", "null-to").foreach { name =>
            statement.setString(1, name)
            statement.setObject(2, if (name == "null-from") { null } else { start.atOffset(ZoneOffset.UTC).toLocalDateTime })
            statement.setObject(3, if (name == "null-to") { null } else { now.atOffset(ZoneOffset.UTC).toLocalDateTime })
            statement.executeUpdate()
          }
        }
        WithClose(cx.prepareStatement(
          s"""INSERT INTO public."$table" (typeName, covered_from, covered_through) VALUES (?, ?, ?)""")) { statement =>
          statement.setString(1, "submillisecond")
          statement.setObject(2, start.plusNanos(1000).atOffset(ZoneOffset.UTC).toLocalDateTime)
          statement.setObject(3, now.atOffset(ZoneOffset.UTC).toLocalDateTime)
          statement.executeUpdate()
        }
        foreach(1 to 3) { i =>
          val asOf = now.plusMillis(i)
          provider.lookup("valid", QueryContext(Filter.INCLUDE, asOf)) must beSome(CoverageInterval("valid", start, now))
          provider.lookup("missing", QueryContext(Filter.INCLUDE, asOf)) must beNone
          provider.lookup("submillisecond", QueryContext(Filter.INCLUDE, asOf)) must throwA[IllegalArgumentException](".*millisecond precision.*")
          provider.lookup("null-from", QueryContext(Filter.INCLUDE, asOf)) must throwA[IllegalArgumentException](".*null bounds.*")
          provider.lookup("null-to", QueryContext(Filter.INCLUDE, asOf)) must throwA[IllegalArgumentException](".*null bounds.*")
        }
        provider.lookup("valid", QueryContext(Filter.INCLUDE, now)) must beSome
        provider.close()
        provider.lookup("valid", QueryContext(Filter.INCLUDE, now.plusSeconds(1))) must throwA[java.sql.SQLException]
      }
      success
    }
  }
}
