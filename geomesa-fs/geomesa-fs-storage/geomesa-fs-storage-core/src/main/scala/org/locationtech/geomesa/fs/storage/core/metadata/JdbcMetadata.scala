/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.{PoolableConnection, PoolingDataSource}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{ColumnBounds, StorageFile}
import org.locationtech.geomesa.fs.storage.core.metadata.JdbcMetadata.{FilesTable, MetadataTable}
import org.locationtech.geomesa.fs.storage.core.metadata.SchemeFilterExtraction.{ColumnBound, ColumnOr}
import org.locationtech.geomesa.fs.storage.core.schemes.ZeroChar
import org.locationtech.geomesa.fs.storage.core.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose

import java.sql._
import java.time.Instant
import scala.Array
import scala.util.control.NonFatal

/**
 * Storage metadata implementation backed by a SQL database. Currently compatible with Postgres - other
 * databases will likely have incompatibilities in the SQL syntax.
 *
 * Schema consists of five tables (note: `storage_` is the default table prefix, but may be customized):
 *
 * `storage_meta`
 *
 * * Holds the base metadata (simple feature type, partition scheme, encoding) as a serialized clob
 *
 * ** root varchar(256) not null
 * ** typeName varchar(256) not null
 * ** key varchar(256) not null,
 * ** value text not null,
 * ** primary key (root, typeName, key)
 *
 * `storage_files`
 *
 * * Holds file-level metadata for each storage file
 *
 * ** id bigint primary key (generated identity)
 * ** root varchar(256) not null
 * ** typeName varchar(256) not null
 * ** file varchar(256) not null
 * ** count bigint not null
 * ** action char(1) not null (A=Append, M=Modify, D=Delete)
 * ** sort integer[]
 * ** ts timestamp without time zone not null
 *
 * `storage_partitions`
 *
 * * Holds partition key-value pairs for each file
 *
 * ** file_id bigint not null (foreign key to storage_files)
 * ** name varchar(64) not null
 * ** value varchar(64) not null
 * ** primary key (file_id, name)
 *
 * `storage_spatial_bounds`
 *
 * * Holds spatial bounds for each file by attribute
 *
 * ** file_id bigint not null (foreign key to storage_files)
 * ** attribute smallint not null
 * ** x_min double precision
 * ** x_max double precision
 * ** y_min double precision
 * ** y_max double precision
 * ** primary key (file_id, attribute)
 *
 * `storage_attr_bounds`
 *
 * * Holds attribute bounds for each file by attribute
 *
 * ** file_id bigint not null (foreign key to storage_files)
 * ** attribute smallint not null
 * ** lower text
 * ** upper text
 * ** primary key (file_id, attribute)
 *
 * @param pool connection pool
 * @param meta metadata table reference
 * @param files files table reference
 * @param namespace feature type namespace
 **/
class JdbcMetadata(
    pool: PoolingDataSource[PoolableConnection],
    meta: MetadataTable,
    files: FilesTable,
    namespace: Option[String],
  ) extends StorageMetadata with SchemeFilterExtraction with LazyLogging {

  // TODO allow for partition changes

  override val `type`: String = JdbcMetadata.MetadataType

  override val sft: SimpleFeatureType =
    WithClose(pool.getConnection())(cx => namespaced(meta.selectFeatureType(cx), namespace))

  override val schemes: Set[PartitionScheme] =
    WithClose(pool.getConnection())(meta.selectPartitionSchemes).map(PartitionSchemeFactory.load(sft, _))

  override def get(key: String): Option[String] = WithClose(pool.getConnection())(meta.select(_, key))

  override def set(key: String, value: String): Unit = {
    WithClose(pool.getConnection()) { cx =>
      if (value == null) {
        meta.delete(cx, key)
      } else {
        meta.insert(cx, key, value)
      }
    }
  }

  override def addFile(file: StorageFile): Unit = addFiles(Seq(file))

  override def addFiles(files: Seq[StorageFile]): Unit = {
    WithClose(pool.getConnection()) { cx =>
      cx.setAutoCommit(false)
      try {
        files.foreach(this.files.insert(cx, _))
        cx.commit()
      } catch {
        case NonFatal(e) =>
          cx.rollback()
          throw e
      } finally {
        cx.setAutoCommit(true)
      }
    }
  }

  override def removeFile(file: StorageFile): Unit = {
    WithClose(pool.getConnection()) { cx =>
      cx.setAutoCommit(false)
      try {
        files.delete(cx, file)
        cx.commit()
      } catch {
        case NonFatal(e) =>
          cx.rollback()
          throw e
      } finally {
        cx.setAutoCommit(true)
      }
    }
  }

  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit = {
    WithClose(pool.getConnection()) { cx =>
      cx.setAutoCommit(false)
      try {
        existing.foreach(files.delete(cx, _))
        replacements.foreach(files.insert(cx, _))
        cx.commit()
      } catch {
        case NonFatal(e) =>
          cx.rollback()
          throw e
      } finally {
        cx.setAutoCommit(true)
      }
    }
  }

  override def getFiles(): Seq[StorageFile] =
    WithClose(pool.getConnection())(files.select(_, Seq.empty, Seq.empty))

  override def getFiles(partition: Partition): Seq[StorageFile] = {
    val filters = partition.values.toSeq.map(p => PartitionRange(p.name, p.value, p.value + ZeroChar))
    WithClose(pool.getConnection())(files.select(_, filters, Seq.empty))
  }

  override def getFiles(filter: Filter): Seq[StorageFile] = {
    if (filter == Filter.INCLUDE) {
      getFiles()
    } else {
      val filters = getFilters(filter)
      if (filters.isEmpty) {
        Seq.empty // no intersecting partitions
      } else {
        WithClose(pool.getConnection()) { cx =>
          filters.flatMap { f =>
            files.select(cx, f.partitions, f.columnBounds)
          }
        }
      }
    }
  }

  override def close(): Unit = pool.close()
}

object JdbcMetadata extends LazyLogging {

  val MetadataType = "jdbc"

  object Config {
    val UrlKey      = "fs.metadata.jdbc.url"
    val SchemaKey   = "fs.metadata.jdbc.schema"
    val PrefixKey   = "fs.metadata.jdbc.table.prefix"
    val DriverKey   = "fs.metadata.jdbc.driver"
    val UserKey     = "fs.metadata.jdbc.user"
    val PasswordKey = "fs.metadata.jdbc.password"

    val MinIdleKey      = "fs.metadata.jdbc.pool.min-idle"
    val MaxIdleKey      = "fs.metadata.jdbc.pool.max-idle"
    val MaxSizeKey      = "fs.metadata.jdbc.pool.max-size"
    val FairnessKey     = "fs.metadata.jdbc.pool.fairness"
    val TestOnBorrowKey = "fs.metadata.jdbc.pool.test-on-borrow"
    val TestOnCreateKey = "fs.metadata.jdbc.pool.test-on-create"
    val TestWhileIdlKey = "fs.metadata.jdbc.pool.test-while-idle"
  }

  case class JdbcMetadataConfig(
      url: String,
      schema: String,
      tablePrefix: String,
      driver: Option[String],
      user: Option[String],
      password: Option[String],
      minIdle: Option[Int],
      maxIdle: Option[Int],
      maxSize: Option[Int],
      fairness: Option[Boolean],
      testOnBorrow: Option[Boolean],
      testOnCreate: Option[Boolean],
      testWhileIdle: Option[Boolean],
    ) {
    require(schema.indexOf('"') == -1, s"Schema must not contain quotes: $schema")
    require(tablePrefix.indexOf('"') == -1, s"Table prefix must not contain quotes: $tablePrefix")
  }

  object JdbcMetadataConfig {
    def apply(config: Map[String, String]): JdbcMetadataConfig =
      JdbcMetadataConfig(
        config.getOrElse(Config.UrlKey, throw new IllegalArgumentException(s"JdbcMetadata requires '${Config.UrlKey}'")),
        config.getOrElse(Config.SchemaKey, "public"),
        config.get(Config.PrefixKey).fold("storage_")(p => if (p.endsWith("_")) { p } else { p + "_" }),
        config.get(Config.DriverKey),
        config.get(Config.UserKey),
        config.get(Config.PasswordKey),
        config.get(Config.MinIdleKey).map(_.toInt),
        config.get(Config.MaxIdleKey).map(_.toInt),
        config.get(Config.MaxSizeKey).map(_.toInt),
        config.get(Config.FairnessKey).map(_.toBoolean),
        config.get(Config.TestOnBorrowKey).map(_.toBoolean),
        config.get(Config.TestOnCreateKey).map(_.toBoolean),
        config.get(Config.TestWhileIdlKey).map(_.toBoolean),
      )
  }

  class MetadataTable(val schema: String, tablePrefix: String, root: String, typeName: String) {

    val tableName: String = s"${tablePrefix}meta"

    private val qualifiedTableName = s""""$schema"."$tableName""""

    def create(cx: Connection): Unit = {
      WithClose(cx.createStatement()) { st =>
        st.executeUpdate(
          s"""create table if not exists $qualifiedTableName (
              root varchar(256) not null,
              typeName varchar(256) not null,
              key varchar(256) not null,
              value text not null,
              primary key (root, typeName, key))"""
        )
      }
    }

    def selectTypeNames(cx: Connection, root: String): Seq[String] = {
      WithClose(cx.prepareStatement(s"SELECT DISTINCT typeName FROM $qualifiedTableName WHERE root = ?")) { st =>
        st.setString(1, root)
        val builder = Seq.newBuilder[String]
        WithClose(st.executeQuery()) { rs =>
          while (rs.next()) {
            builder += rs.getString(1)
          }
        }
        builder.result()
      }
    }

    def insert(cx: Connection, sft: SimpleFeatureType): Unit =
      insert(cx, "__sft__", SimpleFeatureTypes.encodeType(sft, includeUserData = true))

    def insert(cx: Connection, partitions: Seq[String]): Unit = insert(cx, "__partitions__", partitions.mkString(","))

    def insert(cx: Connection, key: String, value: String): Unit = {
      val sql =
        s"INSERT INTO $qualifiedTableName (root, typeName, key, value) " +
          "VALUES (?, ?, ?, ?) ON CONFLICT (root, typeName, key) DO UPDATE SET value = EXCLUDED.value"
      WithClose(cx.prepareStatement(sql)) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        st.setString(3, key)
        st.setString(4, value)
        st.executeUpdate()
      }
    }

    def selectFeatureType(cx: Connection): SimpleFeatureType = SimpleFeatureTypes.createType(typeName, select(cx, "__sft__").get)

    def selectPartitionSchemes(cx: Connection): Set[String] = select(cx, "__partitions__").fold(Set.empty[String])(_.split(",").toSet)

    def select(cx: Connection, key: String): Option[String] = {
      WithClose(cx.prepareStatement(s"SELECT value FROM $qualifiedTableName WHERE root = ? AND typeName = ? AND key = ?")) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        st.setString(3, key)
        WithClose(st.executeQuery()) { rs =>
          if (rs.next()) {
            Option(rs.getString(1))
          } else {
            None
          }
        }
      }
    }

    def delete(cx: Connection, key: String): Unit = {
      WithClose(cx.prepareStatement(s"DELETE FROM $qualifiedTableName WHERE root = ? AND typeName = ? AND key = ?")) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        st.setString(3, key)
        st.executeUpdate()
      }
    }
  }

  /**
    * An add/update/delete partition action. Files associated with each action are stored in the FilesTable
    */
  class FilesTable(val schema: String, tablePrefix: String, root: String, typeName: String) {

    private val filesTable = s""""$schema"."${tablePrefix}files""""
    private val partitionsTable = s""""$schema"."${tablePrefix}partitions""""
    private val columnBoundsTable = s""""$schema"."${tablePrefix}col_bounds""""

    def create(cx: Connection): Unit = {
      WithClose(cx.createStatement()) { st =>
        st.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS $filesTable (
             |  id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
             |  root VARCHAR(256) NOT NULL,
             |  typeName varchar(256) not null,
             |  file VARCHAR(256) NOT NULL,
             |  count BIGINT NOT NULL,
             |  action CHAR(1) NOT NULL,
             |  sort INTEGER[],
             |  ts TIMESTAMP WITHOUT TIME ZONE NOT NULL
             |);""".stripMargin
        )
        st.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS $partitionsTable (
             |  file_id BIGINT NOT NULL,
             |  name VARCHAR(64) NOT NULL,
             |  value VARCHAR(64) NOT NULL,
             |  PRIMARY KEY (file_id, name),
             |  CONSTRAINT fk_storage_file
             |    FOREIGN KEY (file_id)
             |    REFERENCES storage_files(id)
             |    ON DELETE CASCADE
             |);""".stripMargin
        )
        st.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS $columnBoundsTable (
             |  file_id BIGINT NOT NULL,
             |  attribute SMALLINT NOT NULL,
             |  lower TEXT,
             |  upper TEXT,
             |  PRIMARY KEY (file_id, attribute),
             |  CONSTRAINT fk_storage_file
             |    FOREIGN KEY (file_id)
             |    REFERENCES storage_files(id)
             |    ON DELETE CASCADE
             |);""".stripMargin
        )
        st.executeUpdate(s"""CREATE INDEX IF NOT EXISTS ${tablePrefix}partitions_idx_partition ON $partitionsTable(name, value);""")
        st.executeUpdate(s"""CREATE INDEX IF NOT EXISTS ${tablePrefix}col_bounds_idx_bounds ON $columnBoundsTable(attribute, lower, upper);""")
      }
    }

    def insert(cx: Connection, file: StorageFile): Unit = {
      val id = WithClose(cx.prepareStatement(
        s"INSERT INTO $filesTable (root, typeName, file, count, action, sort, ts) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id", Statement.RETURN_GENERATED_KEYS)) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        st.setString(3, file.file)
        st.setLong(4, file.count)
        st.setString(5, file.action.toString.substring(0, 1))
        if (file.sort.isEmpty) {
          st.setNull(6, java.sql.Types.ARRAY)
        } else {
          st.setArray(6, cx.createArrayOf("integer", file.sort.map(Int.box).toArray[AnyRef]))
        }
        st.setTimestamp(7, Timestamp.from(Instant.ofEpochMilli(file.timestamp)))
        st.executeUpdate()
        WithClose(st.getGeneratedKeys) { rs =>
          if (rs.next()) {
            rs.getLong(1)
          } else {
            throw new RuntimeException("Failed to retrieve generated key")
          }
        }
      }
      WithClose(cx.prepareStatement(s"INSERT INTO $partitionsTable (file_id, name, value) VALUES (?, ?, ?)")) { st =>
        file.partition.values.foreach { p =>
          st.setLong(1, id)
          st.setString(2, p.name)
          st.setString(3, p.value)
          st.executeUpdate()
        }
      }
      if (file.bounds.nonEmpty) {
        WithClose(cx.prepareStatement(s"INSERT INTO $columnBoundsTable (file_id, attribute, lower, upper) VALUES (?, ?, ?, ?)")) { st =>
          file.bounds.foreach { bounds =>
            st.setLong(1, id)
            st.setInt(2, bounds.attribute)
            st.setString(3, bounds.lower)
            st.setString(4, bounds.upper)
            st.executeUpdate()
          }
        }
      }
    }

    def delete(cx: Connection, file: StorageFile): Unit = {
      WithClose(cx.prepareStatement(s"DELETE FROM $filesTable WHERE root = ? AND typeName = ? AND file = ?")) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        st.setString(3, file.file)
        st.executeUpdate()
      }
    }

    def select(cx: Connection, partitions: Seq[PartitionRange], columnBounds: Seq[ColumnOr]): Seq[StorageFile] = {

      // build query with multiple joins - one for each partition filter (AND logic)
      val partitionJoins = partitions.zipWithIndex.map {
        case (PartitionRange(name, lower, upper), i) => PartitionJoin(name, lower, upper, s"sp_filter_$i")
      }

      // build column bound filters - one LEFT JOIN per attribute with OR logic for bounds
      val columnBoundJoins = columnBounds.zipWithIndex.map {
        case (ColumnOr(attribute, bounds), i) => ColumnBoundsJoin(attribute, bounds, s"cb_filter_$i")
      }

      val joinClause =
        partitionJoins.map { j =>
          s"JOIN $partitionsTable ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.onClause}"
        } ++
        columnBoundJoins.map { j =>
          s"LEFT JOIN $columnBoundsTable ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.tableAlias}.attribute = ?"
        }

      // build WHERE clause for column bounds
      // if bounds don't exist (NULL), we count it as a match
      val boundsWhereClause = columnBoundJoins.map { j =>
        s"(${j.tableAlias}.file_id IS NULL OR ${j.whereClause})"
      }

      // note: all child tables (partitions, column bounds) are pre-aggregated
      // in subqueries. This ensures each file returns exactly one row with no Cartesian products.
      // The sp_filter and cb_filter joins are used for filtering and must remain on the raw tables.
      val whereClause = if (boundsWhereClause.isEmpty) {
        "WHERE sf.root = ? AND sf.typeName = ?"
      } else {
        s"WHERE sf.root = ? AND sf.typeName = ? AND ${boundsWhereClause.mkString(" AND ")}"
      }

      val query =
        s"""SELECT sf.id, sf.file, sf.count, sf.action, sf.sort, sf.ts,
           |  sp.partition_names, sp.partition_values,
           |  cb.cb_attributes, cb.cb_lowers, cb.cb_uppers
           |FROM $filesTable sf
           |${joinClause.mkString("\n")}
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(name ORDER BY name) as partition_names,
           |    array_agg(value ORDER BY name) as partition_values
           |  FROM $partitionsTable
           |  GROUP BY file_id
           |) sp ON sf.id = sp.file_id
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(attribute ORDER BY attribute) as cb_attributes,
           |    array_agg(lower ORDER BY attribute) as cb_lowers,
           |    array_agg(upper ORDER BY attribute) as cb_uppers
           |  FROM $columnBoundsTable
           |  GROUP BY file_id
           |) cb ON sf.id = cb.file_id
           |$whereClause
           |ORDER BY sf.id DESC""".stripMargin

      WithClose(cx.prepareStatement(query)) { st =>
        var paramIndex = 1
        // set partition join parameters
        partitionJoins.foreach { join =>
          paramIndex += join.apply(st, paramIndex)
        }
        // set column bound join parameters (attribute IDs in JOIN clauses)
        columnBoundJoins.foreach { join =>
          st.setInt(paramIndex, join.attribute)
          paramIndex += 1
        }
        // set root parameter
        st.setString(paramIndex, root)
        paramIndex += 1
        st.setString(paramIndex, typeName)
        paramIndex += 1
        // set column bound WHERE clause parameters
        columnBoundJoins.foreach { join =>
          paramIndex += join.apply(st, paramIndex)
        }
        WithClose(st.executeQuery())(toStorageFiles)
      }
    }

    private def toStorageFiles(rs: ResultSet): Seq[StorageFile] = {
      val result = Seq.newBuilder[StorageFile]

      while (rs.next()) {
        val file = rs.getString(2)
        val count = rs.getLong(3)
        val action = rs.getString(4).charAt(0) match {
          case 'A' => StorageMetadata.StorageFileAction.Append
          case 'M' => StorageMetadata.StorageFileAction.Modify
          case 'D' => StorageMetadata.StorageFileAction.Delete
          case c => throw new IllegalStateException(s"Unknown action: $c")
        }
        val sort = Option(rs.getArray(5)).fold(Seq.empty[Int])(_.getArray.asInstanceOf[Array[Integer]].map(_.intValue()).toSeq)
        val timestamp = rs.getTimestamp(6).toInstant.toEpochMilli

        val partitions = {
          val names = Option(rs.getArray(7)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          val values = Option(rs.getArray(8)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          names.indices.map(i => PartitionKey(names(i), values(i))).toSet
        }

        val bounds = {
          val attributes = Option(rs.getArray(9)).map(_.getArray.asInstanceOf[Array[java.lang.Short]]).getOrElse(Array.empty)
          val lowers = Option(rs.getArray(10)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          val uppers = Option(rs.getArray(11)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          attributes.indices.map(i => ColumnBounds(attributes(i).intValue(), lowers(i), uppers(i)))
        }

        result += StorageFile(file, Partition(partitions), count, action, bounds, sort, timestamp)
      }

      result.result()
    }

    private case class PartitionJoin(name: String, lower: String, upper: String, tableAlias: String) {
      // partitions are lower-bound inclusive and upper-bound exclusive
      def onClause: String = s"$tableAlias.name = ? AND $tableAlias.value >= ? AND $tableAlias.value < ?"
      def apply(st: PreparedStatement, i: Int): Int = {
        st.setString(i, name)
        st.setString(i + 1, lower)
        st.setString(i + 2, upper)
        3
      }
    }

    // attribute bounds filter with OR logic for multiple bounds on the same attribute
    private case class ColumnBoundsJoin(attribute: Int, bounds: Seq[ColumnBound], tableAlias: String) {
      // generate WHERE clause with OR logic for all bounds
      // checks if any of the bounds intersect with the file's attribute bounds
      def whereClause: String = {
        bounds.map { _ =>
          s"($tableAlias.lower <= ? AND $tableAlias.upper >= ?)"
        }.mkString(" OR ")
      }

      def apply(st: PreparedStatement, i: Int): Int = {
        var paramIndex = i
        bounds.foreach { bound =>
          // for intersection: file.lower <= filter.upper AND file.upper >= filter.lower
          st.setString(paramIndex, bound.upper)
          st.setString(paramIndex + 1, bound.lower)
          paramIndex += 2
        }
        bounds.size * 2
      }
    }
  }
}
