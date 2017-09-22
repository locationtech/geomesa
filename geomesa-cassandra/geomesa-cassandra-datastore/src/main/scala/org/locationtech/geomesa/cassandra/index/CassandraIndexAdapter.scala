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

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature, EmptyPlan, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait CassandraIndexAdapter[K]
    extends IndexAdapter[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange], K] {

  this: CassandraFeatureIndex =>

  private val FeatureColumn = NamedColumn("sf", -1, "blob", classOf[ByteBuffer])

  override protected def createInsert(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row) :+ RowValue(FeatureColumn, ByteBuffer.wrap(cf.fullValue))

  override protected def createDelete(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: CassandraFilterStrategyType,
                                  indexValues: Option[K],
                                  ranges: Seq[Seq[RowRange]],
                                  ecql: Option[Filter],
                                  hints: Hints): CassandraQueryPlanType = {
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
}
