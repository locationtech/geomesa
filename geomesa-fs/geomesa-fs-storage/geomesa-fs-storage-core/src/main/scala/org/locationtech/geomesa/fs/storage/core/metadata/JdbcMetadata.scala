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
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{AttributeBounds, SpatialBounds, StorageFile}
import org.locationtech.geomesa.fs.storage.core.metadata.JdbcMetadata.MetadataTable
import org.locationtech.geomesa.fs.storage.core.metadata.SchemeFilterExtraction.{AttributeBound, AttributeOr, SpatialBound, SpatialOr}
import org.locationtech.geomesa.fs.storage.core.schemes.ZeroChar
import org.locationtech.geomesa.fs.storage.core.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.sql._
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
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
 * ** meta text not null
 * ** primary key (root, typeName)
 *
 * `storage_files`
 *
 * * Holds file-level metadata for each storage file
 *
 * ** id bigint primary key (generated identity)
 * ** root varchar(256) not null
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
 * @param root storage root path
 * @param schema database schema name
 * @param tablePrefix table name prefix
 * @param meta basic metadata config
 **/
class JdbcMetadata(
    pool: PoolingDataSource[PoolableConnection],
    root: String,
    schema: String,
    tablePrefix: String,
    meta: Metadata
  ) extends StorageMetadata with SchemeFilterExtraction with LazyLogging {

  // TODO allow for partition changes

  import JdbcMetadata.FilesTable

  import scala.collection.JavaConverters._

  private val metaTable = new MetadataTable(schema, tablePrefix)
  private val filesTable = new FilesTable(schema, tablePrefix)

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)

  override val `type`: String = JdbcMetadata.MetadataType
  override val sft: SimpleFeatureType = meta.sft
  override val schemes: Set[PartitionScheme] = meta.partitions.map(PartitionSchemeFactory.load(sft, _)).toSet

  override def get(key: String): Option[String] = Option(kvs.get(key))

  override def set(key: String, value: String): Unit = {
    kvs.put(key, value)
    WithClose(pool.getConnection()) { connection =>
      metaTable.update(connection, root, meta.copy(config = kvs.asScala.toMap))
    }
  }

  override def addFile(file: StorageFile): Unit = {
    WithClose(pool.getConnection()) { cx =>
      cx.setAutoCommit(false)
      try {
        filesTable.insert(cx, root, file)
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
        filesTable.delete(cx, root, file)
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
        existing.foreach(filesTable.delete(cx, root, _))
        replacements.foreach(filesTable.insert(cx, root, _))
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
    WithClose(pool.getConnection())(filesTable.select(_, root, Seq.empty, Seq.empty, Seq.empty))

  override def getFiles(partition: Partition): Seq[StorageFile] = {
    val filters = partition.values.toSeq.map(p => PartitionRange(p.name, p.value, p.value + ZeroChar))
    WithClose(pool.getConnection())(filesTable.select(_, root, filters, Seq.empty, Seq.empty))
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
            filesTable.select(cx, root, f.partitions, f.spatialBounds, f.attributeBounds)
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

  class MetadataTable(val schema: String, tablePrefix: String) {

    val tableName: String = s"${tablePrefix}meta"

    private val qualifiedTableName = s""""$schema"."$tableName""""

    def create(connection: Connection): Unit = {
      WithClose(connection.createStatement()) { st =>
        st.executeUpdate(
          s"""create table if not exists $qualifiedTableName (
              root varchar(256) not null,
              typeName varchar(256) not null,
              meta text not null,
              primary key (root, typeName))"""
        )
      }
    }

    def insert(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(s"insert into $qualifiedTableName (root, typeName, meta) values (?, ?, ?)")) { st =>
        st.setString(1, root)
        st.setString(2, metadata.sft.getTypeName)
        st.setString(3, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        st.executeUpdate()
      }
    }

    def update(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(s"update $qualifiedTableName set meta= ? where root = ? and typeName = ?")) { st =>
        st.setString(1, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        st.setString(2, root)
        st.setString(3, metadata.sft.getTypeName)
        st.executeUpdate()
      }
    }

    def selectTypeNames(connection: Connection, root: String): Seq[String] = {
      WithClose(connection.prepareStatement(s"select typeName from $qualifiedTableName where root = ?")) { st =>
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

    def selectMetadata(connection: Connection, root: String, typeName: String): Metadata = {
      WithClose(connection.prepareStatement(s"select meta from $qualifiedTableName where root = ? and typeName = ?")) { st =>
        st.setString(1, root)
        st.setString(2, typeName)
        WithClose(st.executeQuery()) { rs =>
          if (rs.next()) {
            MetadataSerialization.deserialize(new ByteArrayInputStream(rs.getString(1).getBytes(StandardCharsets.UTF_8)))
          } else {
            throw new IllegalArgumentException(s"Type '$typeName' does not exist under root $root")
          }
        }
      }
    }
  }

  /**
    * An add/update/delete partition action. Files associated with each action are stored in the FilesTable
    */
  class FilesTable(val schema: String, tablePrefix: String) {

    private val filesTable = s""""$schema"."${tablePrefix}files""""
    private val partitionsTable = s""""$schema"."${tablePrefix}partitions""""
    private val spatialBoundsTable = s""""$schema"."${tablePrefix}spatial_bounds""""
    private val attrBoundsTable = s""""$schema"."${tablePrefix}attr_bounds""""

    def create(cx: Connection): Unit = {
      WithClose(cx.createStatement()) { st =>
        st.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS $filesTable (
             |  id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
             |  root VARCHAR(256) NOT NULL,
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
          s"""CREATE TABLE IF NOT EXISTS $spatialBoundsTable (
             |  file_id BIGINT NOT NULL,
             |  attribute SMALLINT NOT NULL,
             |  x_min DOUBLE PRECISION,
             |  x_max DOUBLE PRECISION,
             |  y_min DOUBLE PRECISION,
             |  y_max DOUBLE PRECISION,
             |  PRIMARY KEY (file_id, attribute),
             |  CONSTRAINT fk_storage_file
             |    FOREIGN KEY (file_id)
             |    REFERENCES storage_files(id)
             |    ON DELETE CASCADE
             |);""".stripMargin
        )
        st.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS $attrBoundsTable (
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
        st.executeUpdate(s"""CREATE INDEX IF NOT EXISTS ${tablePrefix}attr_bounds_idx_bounds ON $attrBoundsTable(attribute, lower, upper);""")
        st.executeUpdate(s"""CREATE INDEX IF NOT EXISTS ${tablePrefix}spatial_bounds_idx_bounds ON $spatialBoundsTable(attribute, x_min, x_max, y_min, y_max);""")
      }
    }

    def insert(cx: Connection, root: String, file: StorageFile): Unit = {
      val id = WithClose(cx.prepareStatement(
        s"INSERT INTO $filesTable (root, file, count, action, sort, ts) " +
          "VALUES (?, ?, ?, ?, ?, ?) RETURNING id", Statement.RETURN_GENERATED_KEYS)) { st =>
        st.setString(1, root)
        st.setString(2, file.file)
        st.setLong(3, file.count)
        st.setString(4, file.action.toString.substring(0, 1))
        if (file.sort.isEmpty) {
          st.setNull(5, java.sql.Types.ARRAY)
        } else {
          st.setArray(5, cx.createArrayOf("integer", file.sort.map(Int.box).toArray[AnyRef]))
        }
        st.setTimestamp(6, Timestamp.from(Instant.ofEpochMilli(file.timestamp)))
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
      if (file.spatialBounds.nonEmpty) {
        WithClose(cx.prepareStatement(s"INSERT INTO $spatialBoundsTable (file_id, attribute, x_min, x_max, y_min, y_max) VALUES (?, ?, ?, ?, ?, ?)")) { st =>
          file.spatialBounds.foreach { bounds =>
            st.setLong(1, id)
            st.setInt(2, bounds.attribute)
            st.setDouble(3, bounds.xmin)
            st.setDouble(4, bounds.xmax)
            st.setDouble(5, bounds.ymin)
            st.setDouble(6, bounds.ymax)
            st.executeUpdate()
          }
        }
      }
      if (file.attributeBounds.nonEmpty) {
        WithClose(cx.prepareStatement(s"INSERT INTO $attrBoundsTable (file_id, attribute, lower, upper) VALUES (?, ?, ?, ?)")) { st =>
          file.attributeBounds.foreach { bounds =>
            st.setLong(1, id)
            st.setInt(2, bounds.attribute)
            st.setString(3, bounds.lower)
            st.setString(4, bounds.upper)
            st.executeUpdate()
          }
        }
      }
    }

    def delete(cx: Connection, root: String, file: StorageFile): Unit = {
      WithClose(cx.prepareStatement(s"DELETE FROM $filesTable WHERE root = ? AND file = ?")) { st =>
        st.setString(1, root)
        st.setString(2, file.file)
        st.executeUpdate()
      }
    }

    def select(
        cx: Connection,
        root: String,
        partitions: Seq[PartitionRange],
        spatialBounds: Seq[SpatialOr],
        attributeBounds: Seq[AttributeOr],
      ): Seq[StorageFile] = {

      // build query with multiple joins - one for each partition filter (AND logic)
      val partitionJoins = partitions.zipWithIndex.map {
        case (PartitionRange(name, lower, upper), i) => PartitionJoin(name, lower, upper, s"sp_filter_$i")
      }

      // build spatial bound filters - one LEFT JOIN per attribute with OR logic for bounds
      val spatialBoundJoins = spatialBounds.zipWithIndex.map {
        case (SpatialOr(attribute, bounds), i) => SpatialBoundsJoin(attribute, bounds, s"sb_filter_$i")
      }

      // build attribute bound filters - one LEFT JOIN per attribute with OR logic for bounds
      val attrBoundJoins = attributeBounds.zipWithIndex.map {
        case (AttributeOr(attribute, bounds), i) => AttributeBoundsJoin(attribute, bounds, s"ab_filter_$i")
      }

      val joinClause =
        partitionJoins.map { j =>
          s"JOIN $partitionsTable ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.onClause}"
        } ++
        spatialBoundJoins.map { j =>
          s"LEFT JOIN $spatialBoundsTable ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.tableAlias}.attribute = ?"
        } ++
        attrBoundJoins.map { j =>
          s"LEFT JOIN $attrBoundsTable ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.tableAlias}.attribute = ?"
        }

      // build WHERE clause for spatial and attribute bounds
      // if bounds don't exist (NULL), we count it as a match
      val spatialWhereClause = spatialBoundJoins.map { j =>
        s"(${j.tableAlias}.file_id IS NULL OR ${j.whereClause})"
      }
      val attrWhereClause = attrBoundJoins.map { j =>
        s"(${j.tableAlias}.file_id IS NULL OR ${j.whereClause})"
      }
      val allWhereClauses = spatialWhereClause ++ attrWhereClause

      // note: all child tables (partitions, spatial bounds, attribute bounds) are pre-aggregated
      // in subqueries. This ensures each file returns exactly one row with no Cartesian products.
      // The sp_filter, sb_filter, and ab_filter joins are used for filtering and must remain on the raw tables.
      val whereClause = if (allWhereClauses.isEmpty) {
        "WHERE sf.root = ?"
      } else {
        s"WHERE sf.root = ? AND ${allWhereClauses.mkString(" AND ")}"
      }

      val query =
        s"""SELECT sf.id, sf.file, sf.count, sf.action, sf.sort, sf.ts,
           |  sp.partition_names, sp.partition_values,
           |  sb.sb_attributes, sb.sb_x_mins, sb.sb_x_maxs, sb.sb_y_mins, sb.sb_y_maxs,
           |  ab.ab_attributes, ab.ab_lowers, ab.ab_uppers
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
           |    array_agg(attribute ORDER BY attribute) as sb_attributes,
           |    array_agg(x_min ORDER BY attribute) as sb_x_mins,
           |    array_agg(x_max ORDER BY attribute) as sb_x_maxs,
           |    array_agg(y_min ORDER BY attribute) as sb_y_mins,
           |    array_agg(y_max ORDER BY attribute) as sb_y_maxs
           |  FROM $spatialBoundsTable
           |  GROUP BY file_id
           |) sb ON sf.id = sb.file_id
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(attribute ORDER BY attribute) as ab_attributes,
           |    array_agg(lower ORDER BY attribute) as ab_lowers,
           |    array_agg(upper ORDER BY attribute) as ab_uppers
           |  FROM $attrBoundsTable
           |  GROUP BY file_id
           |) ab ON sf.id = ab.file_id
           |$whereClause
           |ORDER BY sf.id DESC""".stripMargin

      WithClose(cx.prepareStatement(query)) { st =>
        var paramIndex = 1
        // set partition join parameters
        partitionJoins.foreach { join =>
          paramIndex += join.apply(st, paramIndex)
        }
        // set spatial bound join parameters (attribute IDs in JOIN clauses)
        spatialBoundJoins.foreach { join =>
          st.setInt(paramIndex, join.attribute)
          paramIndex += 1
        }
        // set attribute bound join parameters (attribute IDs in JOIN clauses)
        attrBoundJoins.foreach { join =>
          st.setInt(paramIndex, join.attribute)
          paramIndex += 1
        }
        // set root parameter
        st.setString(paramIndex, root)
        paramIndex += 1
        // set spatial bound WHERE clause parameters
        spatialBoundJoins.foreach { join =>
          paramIndex += join.apply(st, paramIndex)
        }
        // set attribute bound WHERE clause parameters
        attrBoundJoins.foreach { join =>
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

        val spatialBounds = {
          val attributes = Option(rs.getArray(9)).map(_.getArray.asInstanceOf[Array[java.lang.Short]]).getOrElse(Array.empty)
          val xMins = Option(rs.getArray(10)).map(_.getArray.asInstanceOf[Array[java.lang.Double]]).getOrElse(Array.empty)
          val xMaxs = Option(rs.getArray(11)).map(_.getArray.asInstanceOf[Array[java.lang.Double]]).getOrElse(Array.empty)
          val yMins = Option(rs.getArray(12)).map(_.getArray.asInstanceOf[Array[java.lang.Double]]).getOrElse(Array.empty)
          val yMaxs = Option(rs.getArray(13)).map(_.getArray.asInstanceOf[Array[java.lang.Double]]).getOrElse(Array.empty)
          attributes.indices.map { i =>
            SpatialBounds(attributes(i).intValue(), xMins(i).doubleValue(), yMins(i).doubleValue(), xMaxs(i).doubleValue(), yMaxs(i).doubleValue())
          }
        }

        val attributeBounds = {
          val attributes = Option(rs.getArray(14)).map(_.getArray.asInstanceOf[Array[java.lang.Short]]).getOrElse(Array.empty)
          val lowers = Option(rs.getArray(15)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          val uppers = Option(rs.getArray(16)).map(_.getArray.asInstanceOf[Array[String]]).getOrElse(Array.empty)
          attributes.indices.map(i => AttributeBounds(attributes(i).intValue(), lowers(i), uppers(i)))
        }

        result += StorageFile(file, Partition(partitions), count, action, spatialBounds, attributeBounds, sort, timestamp)
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

    // spatial bounds filter with OR logic for multiple bounds on the same attribute
    private case class SpatialBoundsJoin(attribute: Int, bounds: Seq[SpatialBound], tableAlias: String) {
      // generate WHERE clause with OR logic for all bounds
      // checks if any of the bounds intersect with the file's spatial bounds
      def whereClause: String = {
        bounds.map { _ =>
          s"($tableAlias.x_min <= ? AND $tableAlias.x_max >= ? AND $tableAlias.y_min <= ? AND $tableAlias.y_max >= ?)"
        }.mkString(" OR ")
      }

      def apply(st: PreparedStatement, i: Int): Int = {
        var paramIndex = i
        bounds.foreach { bound =>
          // for intersection: file.xmin <= filter.xmax AND file.xmax >= filter.xmin
          st.setDouble(paramIndex, bound.xmax)
          st.setDouble(paramIndex + 1, bound.xmin)
          st.setDouble(paramIndex + 2, bound.ymax)
          st.setDouble(paramIndex + 3, bound.ymin)
          paramIndex += 4
        }
        bounds.size * 4
      }
    }

    // attribute bounds filter with OR logic for multiple bounds on the same attribute
    private case class AttributeBoundsJoin(attribute: Int, bounds: Seq[AttributeBound], tableAlias: String) {
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
