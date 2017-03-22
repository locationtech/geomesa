/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import java.nio.ByteBuffer

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object CassandraFeatureIndex extends CassandraIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[CassandraFeatureIndex] =
    Seq(CassandraZ3Index, CassandraXZ3Index, CassandraZ2Index, CassandraXZ2Index, CassandraIdIndex, CassandraAttributeIndex)

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices

  implicit class RichByteArray(val array: Array[Byte]) extends AnyVal {
    def getOrElse(i: Int, default: Byte): Byte = if (array.length > i) { array(i) } else { default }
  }
}

trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange]]
    with ClientSideFiltering[Row] with LazyLogging {

  private val sfts = new ThreadLocal[SimpleFeatureType]

  private val FeatureColumn = NamedColumn("sf", -1, "blob", classOf[ByteBuffer])

  protected def columns: Seq[NamedColumn]

  protected def rowToColumns(sft: SimpleFeatureType, row: Array[Byte]): Seq[RowValue]
  protected def columnsToRow(columns: Seq[RowValue]): Array[Byte]

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    super.configure(sft, ds)

    val tableName = getTableName(sft.getTypeName, ds)
    val cluster = ds.session.getCluster
    val table = cluster.getMetadata.getKeyspace(ds.session.getLoggedKeyspace).getTable(tableName)

    if (table == null) {
      val (partitions, pks) = columns.partition(_.partition)
      val create = s"CREATE TABLE $tableName (${columns.map(c => s"${c.name} ${c.cType}").mkString(", ")}, sf blob, " +
          s"PRIMARY KEY (${partitions.map(_.name).mkString("(", ", ", ")")}" +
          s"${if (pks.nonEmpty) { pks.map(_.name).mkString(", ", ", ", "")} else { "" }}))"
      logger.debug(create)
      ds.session.execute(create)
    }
  }

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = {
    if (shared) {
      throw new NotImplementedError() // TODO
    } else {
      val tableName = getTableName(sft.getTypeName, ds)
      ds.session.execute(s"drop table $tableName")
    }
  }

  abstract override def getQueryPlan(sft: SimpleFeatureType,
                                     ds: CassandraDataStore,
                                     filter: CassandraFilterStrategyType,
                                     hints: Hints,
                                     explain: Explainer): CassandraQueryPlanType = {
    sfts.set(sft)
    try {
      super.getQueryPlan(sft, ds, filter, hints, explain)
    } finally {
      sfts.remove()
    }
  }

  override protected def createInsert(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row) :+ RowValue(FeatureColumn, ByteBuffer.wrap(cf.fullValue))

  override protected def createDelete(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: CassandraFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[Seq[RowRange]],
                                  ecql: Option[Filter]): CassandraQueryPlanType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    if (ranges.isEmpty) {
      EmptyPlan(filter)
    } else {
      val ks = ds.session.getLoggedKeyspace
      val tableName = getTableName(sft.getTypeName, ds)
      val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
      val statements = ranges.map { criteria =>
        val select = QueryBuilder.select.all.from(ks, tableName)
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
      QueryPlan(filter, tableName, statements, ds.config.queryThreads, ecql, toFeatures)
    }
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Seq[RowRange] = {
    val sft = sfts.get

    val startValues = rowToColumns(sft, start)
    val endValues = rowToColumns(sft, end)

    // TODO avoid zip...
    startValues.zip(endValues).map { case (s, e) => RowRange(s.column, s.value, e.value) }
  }

  override protected def rangeExact(row: Array[Byte]): Seq[RowRange] =
    rowToColumns(sfts.get, row).map { case RowValue(col, v) => RowRange(col, v, v) }

  override def rowAndValue(result: Row): RowAndValue = {
    val values = columns.map(c => RowValue(c, result.get(c.i, c.jType).asInstanceOf[AnyRef]))
    val sf = result.getBytes("sf")

    val row = columnsToRow(values)
    val value = Array.ofDim[Byte](sf.limit())
    sf.get(value)

    RowAndValue(row, 0, row.length, value, 0, value.length)
  }
}
