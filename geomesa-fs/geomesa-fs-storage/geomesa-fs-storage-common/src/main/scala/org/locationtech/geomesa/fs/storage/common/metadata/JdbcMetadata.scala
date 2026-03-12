/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.{PoolableConnection, PoolingDataSource}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionBounds, PartitionFilter, PartitionRange, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{AttributeBounds, Partition, PartitionDimension, SpatialBounds, StorageFile, StorageFileFilter}
import org.locationtech.geomesa.fs.storage.api.{Metadata, PartitionScheme, PartitionSchemeFactory, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.metadata.JdbcMetadata.MetadataTable
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.sql._
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.Array
import scala.util.control.NonFatal

/**
  * Storage metadata implementation backed by a SQL database. Currently tested with Postgres - other
  * databases may have incompatibilities in the SQL syntax.
  *
 * TODO update the docs this isn't right anymore
 *
  * Scheme consists of three tables and one sequence:
  *
  * `storage_meta`
  *
  * * Holds the base metadata (simple feature type, partition scheme, encoding, leaf storage) as a JSON clob
  *
  * ** root varchar(256) not null
  * ** value text not null
  * ** primary key (root)
  *
  * `storage_partitions_id_seq`
  *
  * * Sequence for partition action IDs
  *
  * `storage_partitions`
  *
  * * Holds add/remove partition actions. Tracks the type, count (features) and bounds of each action
  *
  * ** root varchar(256) not null
  * ** name varchar(256) not null
  * ** id int not null
  * ** action char(1) not null
  * ** features bigint
  * ** bounds_xmin double precision
  * ** bounds_xmax double precision
  * ** bounds_ymin double precision
  * ** bounds_ymax double precision
  * ** primary key (root, name, id)
  *
  * `storage_partition_files`
  *
  * * Holds the files associated with each partition action
  *
  * ** root varchar(256) not null
  * ** name varchar(256) not null
  * ** id int not null - foreign key to `storage_partitions`
  * ** file varchar(256) not null
  * ** primary key (root, name, id, file)
  *
  * @param pool connection pool
  * @param root storage root path
  * @param sft simple feature type
  * @param meta basic metadata config
  **/
class JdbcMetadata(
    pool: PoolingDataSource[PoolableConnection],
    root: String,
    val sft: SimpleFeatureType,
    meta: Metadata
  ) extends StorageMetadata with LazyLogging {

  // TODO back compatibility
  // TODO allow for schema changes

  import JdbcMetadata.FilesTable
  import JdbcMetadata.FilesTable.FilesTableFilter

  import scala.collection.JavaConverters._

  override val schemes: Set[PartitionScheme] = meta.partitions.map(PartitionSchemeFactory.load(sft, _)).toSet
  override val encoding: String = meta.config(Metadata.Encoding)

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)

  override def get(key: String): Option[String] = Option(kvs.get(key))

  override def set(key: String, value: String): Unit = {
    kvs.put(key, value)
    WithClose(pool.getConnection()) { connection =>
      MetadataTable.update(connection, root, meta.copy(config = kvs.asScala.toMap))
    }
  }

  override def addFile(file: StorageFile): Unit =
    WithClose(pool.getConnection())(FilesTable.insert(_, root, file))

  override def removeFile(file: StorageFile): Unit =
    WithClose(pool.getConnection())(FilesTable.delete(_, root, file))

  override def getFiles(): Seq[StorageFile] =
    WithClose(pool.getConnection())(FilesTable.select(_, root, Seq.empty, Seq.empty, Seq.empty))

  override def getFiles(partition: Partition): Seq[StorageFile] = {
    val filters = partition.dims.toSeq.map(p => SinglePartition(p.name, p.value))
    WithClose(pool.getConnection())(FilesTable.select(_, root, filters, Seq.empty, Seq.empty))
  }

  override def getFiles(filter: Filter): Seq[StorageFileFilter] = {
    // TODO enforce that files have at least one partition? or verify that logic here doesn't assumes that
    getFilters(filter) match {
      case None =>
        logger.info(s"no filter extracted from ${filter}")
        val spatialBounds = extractGeometries(filter)
        val attributeBounds = extractAttributes(filter)
        WithClose(pool.getConnection()) { cx =>
          FilesTable.select(cx, root, Seq.empty, spatialBounds, attributeBounds).map(StorageFileFilter(_, Some(filter)))
        }

      case Some(filters) =>
        logger.info(s"extracted $filters from ${filter}")
        if (filters.isEmpty) {
          Seq.empty // no intersecting partitions
        } else {
          WithClose(pool.getConnection()) { cx =>
            filters.flatMap { f =>
              FilesTable.select(cx, root, f.partitionsAnd, f.spatialBoundsOr, f.attributeBoundsOr).map(StorageFileFilter(_, f.filter))
            }
          }
        }
    }
  }

  private def getFilters(filter: Filter): Option[Seq[FilesTableFilter]] = {
    val iter = schemes.iterator
    var start: Option[Seq[PartitionFilter]] = None
    while (start.isEmpty && iter.hasNext) {
      start = iter.next().getIntersectingPartitions(filter)
    }
    start.map { ranges =>
      val filters = ranges.flatMap { range =>
        range.bounds.map { bound =>
          FilesTableFilter(range.filter, Seq(bound), Seq.empty, Seq.empty)
        }
      }
      iter.foldLeft(filters) { case (filters, scheme) => addPartition(scheme, filters) }.map { fileTableFilter =>
        val spatialBounds = fileTableFilter.filter.fold(Seq.empty[SpatialBounds])(extractGeometries)
        val attributeBounds = fileTableFilter.filter.fold(Seq.empty[AttributeBounds])(extractAttributes)
        fileTableFilter.copy(spatialBoundsOr = spatialBounds, attributeBoundsOr = attributeBounds)
      }
    }
  }

  private def addPartition(scheme: PartitionScheme, filters: Seq[FilesTableFilter]): Seq[FilesTableFilter] = {
    val result = Seq.newBuilder[FilesTableFilter]
    filters.foreach { filesTableFilter =>
      filesTableFilter.filter.foreach { f =>
        scheme.getIntersectingPartitions(f) match {
          case None => result += filesTableFilter
          case Some(ranges) =>
            ranges.foreach { range =>
              range.bounds.foreach { bound =>
                result += filesTableFilter.copy(range.filter, filesTableFilter.partitionsAnd :+ bound)
              }
            }
        }
      }
    }
    result.result()
  }

  private def extractGeometries(filter: Filter): Seq[SpatialBounds] = {
    sft.getAttributeDescriptors.asScala.toSeq
      .filter(_ == sft.getGeometryDescriptor) // TODO non-default geom bounds
      .flatMap { d =>
        FilterHelper.extractGeometries(filter, d.getLocalName).values.flatMap { g =>
          SpatialBounds(sft.indexOf(d.getLocalName), g.getEnvelopeInternal)
        }
      }
  }

  private def extractAttributes(filter: Filter): Seq[AttributeBounds] = {
    sft.getAttributeDescriptors.asScala.toSeq
      .filter(_ => false) // TODO attribute bounds
      .flatMap { d =>
        FilterHelper.extractAttributeBounds(filter, d.getLocalName, d.getType.getBinding).values.map { b =>
          val lower = b.lower.value.map(AttributeIndexKey.typeEncode).getOrElse("")
          val upper = b.upper.value.map(AttributeIndexKey.typeEncode).getOrElse("zzz")
          AttributeBounds(sft.indexOf(d.getLocalName), lower, upper)
        }
      }
  }

  override def close(): Unit = pool.close()
}

object JdbcMetadata extends LazyLogging {

  val MetadataType = "jdbc"

  object Config {
    val UrlKey      = "jdbc.url"
    val DriverKey   = "jdbc.driver"
    val UserKey     = "jdbc.user"
    val PasswordKey = "jdbc.password"

    val MinIdleKey      = "jdbc.pool.min-idle"
    val MaxIdleKey      = "jdbc.pool.max-idle"
    val MaxSizeKey      = "jdbc.pool.max-size"
    val FairnessKey     = "jdbc.pool.fairness"
    val TestOnBorrowKey = "jdbc.pool.test-on-borrow"
    val TestOnCreateKey = "jdbc.pool.test-on-create"
    val TestWhileIdlKey = "jdbc.pool.test-while-idle"
  }

  private val RootCol = "\"root\""

  private object MetadataTable {

    private val TableName = "storage_meta"

    private val ValueCol = "\"value\""

    private val CreateStatement: String =
      s"create table if not exists $TableName (" +
          s"$RootCol varchar(256) not null, " +
          s"$ValueCol text not null, " +
          s"primary key ($RootCol))"

    private val InsertStatement: String = s"insert into $TableName ($RootCol, $ValueCol) values (?, ?)"

    private val UpdateStatement: String = s"update $TableName set $ValueCol = ? where $RootCol = ?"

    private val SelectStatement: String = s"select $ValueCol from $TableName where $RootCol = ?"

    def create(connection: Connection): Unit =
      WithClose(connection.createStatement())(_.executeUpdate(CreateStatement))

    def insert(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(InsertStatement)) { statement =>
        statement.setString(1, root)
        statement.setString(2, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        statement.executeUpdate()
      }
    }

    def update(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(UpdateStatement)) { statement =>
        statement.setString(1, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        statement.setString(2, root)
        statement.executeUpdate()
      }
    }

    def select(connection: Connection, root: String): Option[Metadata] = {
      try {
        WithClose(connection.prepareStatement(SelectStatement)) { statement =>
          statement.setString(1, root)
          WithClose(statement.executeQuery()) { results =>
            if (!results.next) {
              None
            } else {
              val serialized = new ByteArrayInputStream(results.getString(1).getBytes(StandardCharsets.UTF_8))
              Some(MetadataSerialization.deserialize(serialized))
            }
          }
        }
      } catch {
        case e: SQLException =>
          // not sure of a better way to check the existence of a table for an arbitrary SQL database
          // solutions found online didn't work, at least for h2
          logger.debug("Error loading metadata (table may not exist?):", e); None
      }
    }
  }

  /**
    * An add/update/delete partition action. Files associated with each action are stored in the FilesTable
    */
  private object FilesTable {

    def create(cx: Connection): Unit = {
      WithClose(cx.createStatement()) { st =>
        st.executeUpdate(
          """CREATE TABLE IF NOT EXISTS storage_files (
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
          """CREATE TABLE IF NOT EXISTS storage_partitions (
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
          """CREATE TABLE IF NOT EXISTS storage_spatial_bounds (
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
          """CREATE TABLE IF NOT EXISTS storage_attr_bounds (
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
        st.executeUpdate("""CREATE INDEX storage_partitions_idx_partition ON storage_partitions(name, value);""")
        st.executeUpdate("""CREATE INDEX storage_attr_bounds_idx_bounds ON storage_attr_bounds(attribute, lower, upper);""")
        st.executeUpdate("""CREATE INDEX storage_spatial_bounds_idx_bounds ON storage_spatial_bounds(attribute, x_min, x_max, y_min, y_max);""")
      }
    }

    def insert(cx: Connection, root: String, file: StorageFile): Unit = {
      cx.setAutoCommit(false)
      try {
        val id = WithClose(cx.prepareStatement(
          "INSERT INTO storage_files (root, file, count, action, sort, ts) " +
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
        WithClose(cx.prepareStatement("INSERT INTO storage_partitions (file_id, name, value) VALUES (?, ?, ?)")) { st =>
          file.partition.dims.foreach { p =>
            st.setLong(1, id)
            st.setString(2, p.name)
            st.setString(3, p.value)
            st.executeUpdate()
          }
        }
        if (file.spatialBounds.nonEmpty) {
          WithClose(cx.prepareStatement("INSERT INTO storage_spatial_bounds (file_id, attribute, x_min, x_max, y_min, y_max) VALUES (?, ?, ?, ?, ?, ?)")) { st =>
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
          WithClose(cx.prepareStatement("INSERT INTO storage_attr_bounds (file_id, attribute, lower, upper) VALUES (?, ?, ?, ?)")) { st =>
            file.attributeBounds.foreach { bounds =>
              st.setLong(1, id)
              st.setInt(2, bounds.attribute)
              st.setString(3, bounds.lower)
              st.setString(4, bounds.upper)
              st.executeUpdate()
            }
          }
        }
        cx.commit()
      } catch {
        case NonFatal(e) =>
          cx.rollback()
          throw e
      } finally {
        cx.setAutoCommit(true)
      }
    }

    def delete(cx: Connection, root: String, file: StorageFile): Unit = {
      cx.setAutoCommit(false)
      try {
        WithClose(cx.prepareStatement("DELETE FROM storage_files WHERE root = ? AND file = ?")) { st =>
          st.setString(1, root)
          st.setString(2, file.file)
          st.executeUpdate()
        }
        cx.commit()
      } catch {
        case NonFatal(e) =>
          cx.rollback()
          throw e
      } finally {
        cx.setAutoCommit(true)
      }
    }

    def select(
        cx: Connection,
        root: String,
        partitionsAnd: Seq[PartitionBounds],
        spatialBoundsOr: Seq[SpatialBounds],
        attributeBoundsOr: Seq[AttributeBounds],
      ): Seq[StorageFile] = {

      // TODO filter on spatial/attr bounds
      // TODO need to ensure that if spatial/attribute bounds don't exist in the table we count that as a match

      // build query with multiple joins - one for each partition filter (AND logic)
      val partitionJoins = partitionsAnd.zipWithIndex.map {
        case (SinglePartition(name, value), i) => SingleValueJoin(name, value, s"sp_filter_$i")
        case (PartitionRange(name, lower, upper), i) => RangeJoin(name, lower, upper, s"sp_filter_$i")
      }
      val joinClause = partitionJoins.map { j =>
        s"JOIN storage_partitions ${j.tableAlias} ON sf.id = ${j.tableAlias}.file_id AND ${j.onClause}"
      }
logger.info(joinClause.mkString("\n"))
      // note: all child tables (partitions, spatial bounds, attribute bounds) are pre-aggregated
      // in subqueries. This ensures each file returns exactly one row with no Cartesian products.
      // The sp_filter joins are used for filtering and must remain on the raw partition table.
      val query =
        s"""SELECT sf.id, sf.file, sf.count, sf.action, sf.sort, sf.ts,
           |  sp.partition_names, sp.partition_values,
           |  sb.sb_attributes, sb.sb_x_mins, sb.sb_x_maxs, sb.sb_y_mins, sb.sb_y_maxs,
           |  ab.ab_attributes, ab.ab_lowers, ab.ab_uppers
           |FROM storage_files sf
           |${joinClause.mkString("\n")}
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(name ORDER BY name) as partition_names,
           |    array_agg(value ORDER BY name) as partition_values
           |  FROM storage_partitions
           |  GROUP BY file_id
           |) sp ON sf.id = sp.file_id
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(attribute ORDER BY attribute) as sb_attributes,
           |    array_agg(x_min ORDER BY attribute) as sb_x_mins,
           |    array_agg(x_max ORDER BY attribute) as sb_x_maxs,
           |    array_agg(y_min ORDER BY attribute) as sb_y_mins,
           |    array_agg(y_max ORDER BY attribute) as sb_y_maxs
           |  FROM storage_spatial_bounds
           |  GROUP BY file_id
           |) sb ON sf.id = sb.file_id
           |LEFT JOIN (
           |  SELECT file_id,
           |    array_agg(attribute ORDER BY attribute) as ab_attributes,
           |    array_agg(lower ORDER BY attribute) as ab_lowers,
           |    array_agg(upper ORDER BY attribute) as ab_uppers
           |  FROM storage_attr_bounds
           |  GROUP BY file_id
           |) ab ON sf.id = ab.file_id
           |WHERE sf.root = ?
           |ORDER BY sf.id DESC""".stripMargin

      WithClose(cx.prepareStatement(query)) { st =>
        var paramIndex = 1
        partitionJoins.foreach { join =>
          paramIndex += join.apply(st, paramIndex)
        }
        st.setString(paramIndex, root)
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
          names.indices.map(i => PartitionDimension(names(i), values(i))).toSet
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

    case class FilesTableFilter(
        filter: Option[Filter],
        partitionsAnd: Seq[PartitionBounds],
        spatialBoundsOr: Seq[SpatialBounds],
        attributeBoundsOr: Seq[AttributeBounds]
      )

    private trait PartitionJoin {
      def tableAlias: String
      def onClause: String
      def apply(st: PreparedStatement, i: Int): Int
    }

    private case class SingleValueJoin(name: String, value: String, tableAlias: String) extends PartitionJoin {
      override def onClause: String = s"$tableAlias.name = ? AND $tableAlias.value = ?"
      override def apply(st: PreparedStatement, i: Int): Int = {
        st.setString(i, name)
        st.setString(i + 1, value)
        2
      }
    }

    private case class RangeJoin(name: String, lower: String, upper: String, tableAlias: String) extends PartitionJoin {
      override def onClause: String = s"$tableAlias.name = ? AND $tableAlias.value >= ? AND $tableAlias.value < ?"
      override def apply(st: PreparedStatement, i: Int): Int = {
        st.setString(i, name)
        st.setString(i + 1, lower)
        st.setString(i + 2, upper)
        3
      }
    }
  }

  /**
    * Loads metadata from an existing table
    *
    * @param pool connection pool
    * @param root root path
    * @return
    */
  def load(pool: PoolingDataSource[PoolableConnection], root: String): Option[Metadata] =
    WithClose(pool.getConnection())(MetadataTable.select(_, root))

  /**
    * Persists metadata into a new table
    *
    * @param pool connection pool
    * @param root root path
    * @param metadata simple feature type, file encoding, partition scheme, etc
    */
  def create(pool: PoolingDataSource[PoolableConnection], root: String, metadata: Metadata): Unit = {
    WithClose(pool.getConnection()) { cx =>
      MetadataTable.create(cx)
      MetadataTable.insert(cx, root, metadata)
      FilesTable.create(cx)
    }
  }
}
