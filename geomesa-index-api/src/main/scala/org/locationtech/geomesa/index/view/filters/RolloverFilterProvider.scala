/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view.filters

import org.apache.commons.dbcp2.BasicDataSource
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.view.MergedFilterProvider
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.sql.Connection
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import java.util.Date

/**
 * Filter provider for "rollover" scenarios. When using tiered storage, features may be migrated from one store to another
 * on a schedule. This provider can read the work log of the rollover process to ensure that the correct boundaries
 * are enforced between the different stores, based on feature timestamps.
 */
object RolloverFilterProvider {

  import FilterHelper.ff

  private val StateKey = getClass.getName

  /**
   * Create a coverage table if it does not already exist. The caller manages the connection and transaction.
   * Timestamps are stored without a time zone, using UTC wall-clock values for conversion to and from Instant.
   *
   * @param cx connection
   * @param table table name, optionally qualified with a schema (e.g. `public.rollover`)
   */
  def createTable(cx: Connection, table: String): Unit = {
    val qualifiedTable = quoteTable(table)
    WithClose(cx.createStatement()) { st =>
      st.executeUpdate(
        s"CREATE TABLE IF NOT EXISTS $qualifiedTable (" +
          "typeName text NOT NULL, " +
          "covered_from timestamp without time zone NOT NULL, " +
          "covered_through timestamp without time zone NOT NULL, " +
          "PRIMARY KEY (typeName))")
    }
  }

  /**
   * Insert or replace the coverage interval for a type name. The caller manages the connection and transaction.
   *
   * @param connection connection
   * @param table table name, optionally qualified with a schema
   * @param interval coverage interval
   */
  def insertCoverage(connection: Connection, table: String, interval: CoverageInterval): Unit = {
    val qualifiedTable = quoteTable(table)
    WithClose(connection.prepareStatement(
      s"INSERT INTO $qualifiedTable (typeName, covered_from, covered_through) VALUES (?, ?, ?) " +
        "ON CONFLICT (typeName) DO UPDATE SET " +
        "covered_from = EXCLUDED.covered_from, covered_through = EXCLUDED.covered_through")) { statement =>
      statement.setString(1, interval.typeName)
      statement.setObject(2, interval.from.atOffset(ZoneOffset.UTC).toLocalDateTime)
      statement.setObject(3, interval.through.atOffset(ZoneOffset.UTC).toLocalDateTime)
      statement.executeUpdate()
    }
  }

  private def quoteTable(table: String): String = {
    require(table != null, "Table name must not be null")
    val parts = table.split("\\.", -1)
    require(parts.length <= 2, s"Invalid table name: $table")
    parts.map(quoteIdentifier).mkString(".")
  }

  private def quoteIdentifier(identifier: String): String = {
    require(identifier != null && identifier.matches("[A-Za-z_][A-Za-z0-9_]{0,62}"),
      s"Invalid SQL identifier: $identifier")
    "\"" + identifier + "\""
  }

  case class CoverageInterval(typeName: String, from: Instant, through: Instant) {
    require(typeName != null, "Coverage type name must not be null")
    require(from != null && through != null, "Coverage interval must not have null bounds")
    require(from.isBefore(through), "Coverage interval must have from < through")
    require(from.getNano % 1000000 == 0 && through.getNano % 1000000 == 0, "Coverage bounds must have millisecond precision")
  }

  class SourceProvider extends JdbcRolloverProvider {
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
      coverage(sft, context).map { interval =>
        val dtg = ff.property(dtgField(sft))
        val start = ff.literal(Date.from(interval.from))
        val end = ff.literal(Date.from(interval.through))
        ff.or(ff.isNull(dtg), ff.or(ff.less(dtg, start), ff.greaterOrEqual(dtg, end)))
      }
    }
  }

  class DestinationProvider extends JdbcRolloverProvider {
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
      val filter = coverage(sft, context).fold[Filter](Filter.EXCLUDE) { interval =>
        val dtg = ff.property(dtgField(sft))
        val start = ff.literal(Date.from(interval.from))
        val end = ff.literal(Date.from(interval.through))
        ff.and(ff.greaterOrEqual(dtg, start), ff.less(dtg, end))
      }
      Some(filter)
    }
  }

  trait JdbcRolloverProvider extends MergedFilterProvider {

    private var source: BasicDataSource = _
    private var sql: String = _
    private var url: String = _
    private var table: String = _

    override def init(config: Map[String, String]): Unit = {
      def required(k: String): String = config.getOrElse(k, throw new IllegalArgumentException(s"Missing required key: $k"))

      this.url = required("url")
      this.table = s"${quoteIdentifier(required("schema"))}.${quoteIdentifier(required("table"))}"
      this.sql = s"SELECT covered_from, covered_through FROM $table WHERE typeName = ?"
      source = new BasicDataSource()
      source.setUrl(url)
      source.setUsername(required("username"))
      source.setPassword(required("password"))
      source.setMaxTotal(config.getOrElse("max-connections", "2").toInt)
      source.setMaxIdle(1)
      source.setMinIdle(0)
      source.setMaxWait(Duration.ofSeconds(5))
    }

    protected def coverage(sft: SimpleFeatureType, context: QueryContext): Option[CoverageInterval] = {
      val state = context.state.compute(StateKey, (_, existing) => {
        if (existing == null) {
          CoverageState(url, table, dtgField(sft), readCoverage(sft.getTypeName))
        } else {
          validate(sft, existing.asInstanceOf[CoverageState])
        }
      })
      state.asInstanceOf[CoverageState].coverage
    }

    private def validate(sft: SimpleFeatureType, existing: CoverageState): CoverageState = {
      val dtg = dtgField(sft)
      if (existing.url != url || existing.table != table) {
        throw new IllegalArgumentException(
          s"RolloverFilterProviders are configured to read from different tables: " +
            s"${existing.url} ${existing.table} vs $url $table")
      } else if (existing.dtg != dtg) {
        throw new IllegalArgumentException(
          s"RolloverFilterProviders for '${sft.getTypeName}' are using different default date fields: ${existing.dtg} vs $dtg")
      }
      existing
    }

    private def readCoverage(typeName: String): Option[CoverageInterval] = {
      WithClose(source.getConnection) { cx =>
        WithClose(cx.prepareStatement(sql)) { statement =>
          statement.setQueryTimeout(5)
          statement.setString(1, typeName)
          WithClose(statement.executeQuery()) { results =>
            if (!results.next()) {
              None
            } else {
              val from = Option(results.getObject(1, classOf[LocalDateTime])).map(_.toInstant(ZoneOffset.UTC)).orNull
              val through = Option(results.getObject(2, classOf[LocalDateTime])).map(_.toInstant(ZoneOffset.UTC)).orNull
              Some(CoverageInterval(typeName, from, through))
            }
          }
        }
      }
    }

    override def close(): Unit = CloseWithLogging(Option(source))
  }

  /**
   * Previously retrieved coverage for a given query
   *
   * @param url jdbc url
   * @param table qualified table name
   * @param dtg dtg field
   * @param coverage coverage
   */
  private case class CoverageState(url: String, table: String, dtg: String, coverage: Option[CoverageInterval])
}
