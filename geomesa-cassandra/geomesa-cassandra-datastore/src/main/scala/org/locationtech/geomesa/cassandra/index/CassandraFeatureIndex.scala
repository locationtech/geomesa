/***********************************************************************
 * Copyright (c) 2017-2018 IBM
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.index.legacy.{CassandraAttributeIndexV1, CassandraZ2IndexV1, CassandraZ3IndexV1}
import org.locationtech.geomesa.cassandra.{RowValue, _}
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexKeySpace, ShardStrategy}
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object CassandraFeatureIndex extends CassandraIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[CassandraFeatureIndex[_, _]] =
    Seq(CassandraZ3Index, CassandraZ3IndexV1, CassandraXZ3Index, CassandraZ2Index, CassandraZ2IndexV1,
      CassandraXZ2Index, CassandraIdIndex, CassandraAttributeIndex, CassandraAttributeIndexV1)

  override val CurrentIndices: Seq[CassandraFeatureIndex[_, _]] =
    Seq(CassandraZ3Index, CassandraXZ3Index, CassandraZ2Index, CassandraXZ2Index,
      CassandraIdIndex, CassandraAttributeIndex)

  // need to be lazy to prevent instantiation order errors
  lazy val FeatureIdColumnName = "fid"
  lazy val ShardColumn = NamedColumn("shard", 0, "tinyint", classOf[Byte], partition = true)

  def binColumn(i: Int): NamedColumn = NamedColumn("period", i, "smallint", classOf[Short], partition = true)
  def zColumn(i: Int): NamedColumn = NamedColumn("z", i, "bigint", classOf[Long])

  def featureIdColumn(i: Int): NamedColumn = NamedColumn(FeatureIdColumnName, i, "text", classOf[String])
  def featureColumn(i: Int): NamedColumn = NamedColumn("sf", i, "blob", classOf[ByteBuffer])

  def statement(keyspace: String, table: String, criteria: Seq[RowRange]): Select = {
    val select = QueryBuilder.select.all.from(keyspace, table)
    criteria.foreach { c =>
      if (c.start == c.end) {
        if (c.start != null) {
          select.where(QueryBuilder.eq(c.column.name, c.start))
        }
      } else {
        if (c.start != null) {
          select.where(QueryBuilder.gte(c.column.name, c.start))
        }
        if (c.end != null) {
          select.where(QueryBuilder.lt(c.column.name, c.end))
        }
      }
    }
    select
  }
}

trait CassandraFeatureIndex[T, U] extends CassandraFeatureIndexType with ClientSideFiltering[Row] with LazyLogging {

  protected def columns: Seq[NamedColumn]

  /**
    * Primary key space used by this index
    *
    * @return
    */
  protected def keySpace: IndexKeySpace[T, U]

  /**
    * Strategy for sharding
    *
    * @param sft simple feature type
    * @return
    */
  protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy

  /**
    * Convert a generic scan into a c* select
    *
    * @param sft simple feature type
    * @param range range to scan
    * @return
    */
  protected def toRowRanges(sft: SimpleFeatureType, range: ScanRange[U]): Seq[RowRange]

  /**
    * Create values for insert
    *
    * @param shards sharding
    * @param toIndexKey function to create primary keys
    * @param includeFeature include the feature (for insert) or not (for delete)
    * @param cf feature
    * @return
    */
  protected def createValues(shards: ShardStrategy,
                             toIndexKey: SimpleFeature => Seq[U],
                             includeFeature: Boolean)
                            (cf: CassandraFeature): Seq[Seq[RowValue]]

  override def supports(sft: SimpleFeatureType): Boolean = keySpace.supports(sft)

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore, partition: Option[String]): String = {
    val table = super.configure(sft, ds, partition)
    val cluster = ds.session.getCluster

    if (cluster.getMetadata.getKeyspace(ds.session.getLoggedKeyspace).getTable(table) == null) {
      val (partitions, pks) = columns.partition(_.partition)
      val create = s"CREATE TABLE $table (${columns.map(c => s"${c.name} ${c.cType}").mkString(", ")}, " +
          s"PRIMARY KEY (${partitions.map(_.name).mkString("(", ", ", ")")}" +
          s"${if (pks.nonEmpty) { pks.map(_.name).mkString(", ", ", ", "")} else { "" }}))"
      logger.debug(create)
      try { ds.session.execute(create) } catch {
        case _: AlreadyExistsException => // ignore, another thread created it for us
      }
    }

    table
  }

  override def writer(sft: SimpleFeatureType, ds: CassandraDataStore): CassandraFeature => Seq[Seq[RowValue]] = {
    val shards = shardStrategy(sft)
    val toIndexKey: SimpleFeature => Seq[U] = keySpace.toIndexKey(sft)
    createValues(shards, toIndexKey, includeFeature = true)
  }

  override def remover(sft: SimpleFeatureType, ds: CassandraDataStore): CassandraFeature => Seq[Seq[RowValue]] = {
    val shards = shardStrategy(sft)
    val toIndexKey = keySpace.toIndexKey(sft, lenient = true)
    createValues(shards, toIndexKey, includeFeature = false)
  }

  // note: this is used by ClientSideFiltering for extracting the feature id -
  // we always just return the feature ID as the row, so this doesn't vary between indices
  // TODO refactor clientSideFiltering so we don't have to go string -> bytes -> string
  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String =
    bytesToString

  private def bytesToString(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String =
    new String(row, offset, length, StandardCharsets.UTF_8)

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] =
    throw new NotImplementedError("Cassandra does not support splits")

  override def removeAll(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    if (TablePartition.partitioned(sft)) {
      // partitioned indices can just drop the partitions
      delete(sft, ds, None)
    } else {
      // note: tables are never shared
      getTableNames(sft, ds, None).foreach { table =>
        val truncate = s"TRUNCATE $table"
        logger.debug(truncate)
        ds.session.execute(truncate)
      }
    }
  }

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, partition: Option[String]): Unit = {
    // note: tables are never shared
    getTableNames(sft, ds, partition).foreach { table =>
      val delete = s"DROP TABLE IF EXISTS $table"
      logger.debug(delete)
      ds.session.execute(delete)
    }

    // deletes the metadata
    super.delete(sft, ds, partition)
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: CassandraDataStore,
                            filter: CassandraFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): CassandraQueryPlanType = {
    val indexValues = filter.primary.map(keySpace.getIndexValues(sft, _, explain))

    val ranges: Iterator[Seq[RowRange]] = indexValues match {
      case None =>
        // check that full table scans are allowed
        QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan on $name index for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        Iterator.single(Seq.empty)

      case Some(values) =>
        val ranges = keySpace.getRanges(values)
        val splits = shardStrategy(sft).shards.map(b => RowRange(CassandraFeatureIndex.ShardColumn, b(0), b(0)))

        if (splits.isEmpty) {
          ranges.map(toRowRanges(sft, _))
        } else {
          ranges.flatMap { r =>
            val rows = toRowRanges(sft, r)
            splits.map(split => rows.+:(split))
          }
        }
    }

    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints

      val ks = ds.session.getLoggedKeyspace
      val tables = getTablesForQuery(sft, ds, filter.filter)

      val statements = tables.flatMap(table => ranges.map(CassandraFeatureIndex.statement(ks, table, _)))

      val useFullFilter = keySpace.useFullFilter(indexValues, Some(ds.config), hints)
      val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }
      val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
      StatementPlan(filter, tables, statements, ds.config.queryThreads, ecql, toFeatures)
    }
  }

  override def rowAndValue(result: Row): RowAndValue = {
    import CassandraFeatureIndex.FeatureIdColumnName

    val fid = result.get(FeatureIdColumnName, classOf[String]).getBytes(StandardCharsets.UTF_8)

    val sf = result.getBytes("sf")
    val value = Array.ofDim[Byte](sf.limit())
    sf.get(value)

    // note: we just return the feature id as the row
    RowAndValue(fid, 0, fid.length, value, 0, value.length)
  }
}
