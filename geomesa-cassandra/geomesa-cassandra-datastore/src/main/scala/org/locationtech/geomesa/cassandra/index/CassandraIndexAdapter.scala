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

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature, EmptyPlan, QueryPlan}
import org.locationtech.geomesa.cassandra.index.CassandraIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait CassandraIndexAdapter
    extends IndexAdapter[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange], ScanConfig] {

  this: CassandraFeatureIndex =>

  private val FeatureColumn = NamedColumn("sf", -1, "blob", classOf[ByteBuffer])

  override protected def createInsert(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row) :+ RowValue(FeatureColumn, ByteBuffer.wrap(cf.fullValue))

  override protected def createDelete(row: Array[Byte], cf: CassandraFeature): Seq[RowValue] =
    rowToColumns(cf.feature.getFeatureType, row)

  override protected def range(start: Array[Byte], end: Array[Byte]): Seq[RowRange] = {
    val sft = sfts.get

    val startValues = rowToColumns(sft, start)
    val endValues = rowToColumns(sft, end)

    // TODO avoid zip...
    startValues.zip(endValues).map { case (s, e) => RowRange(s.column, s.value, e.value) }
  }

  override protected def rangeExact(row: Array[Byte]): Seq[RowRange] =
    rowToColumns(sfts.get, row).map { case RowValue(col, v) => RowRange(col, v, v) }

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: CassandraFilterStrategyType,
                                  config: ScanConfig): CassandraQueryPlanType = {
    if (config.statements.isEmpty) {
      EmptyPlan(filter)
    } else {
      QueryPlan(filter, config.tableName, config.statements, ds.config.queryThreads, config.ecql, config.toFeatures)
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: CassandraDataStore,
                                    filter: CassandraFilterStrategyType,
                                    ranges: Seq[Seq[RowRange]],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val tableName = getTableName(sft.getTypeName, ds)
    val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
    val ks = ds.session.getLoggedKeyspace
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
    ScanConfig(tableName, statements, ecql, toFeatures)
  }
}

object CassandraIndexAdapter {
  case class ScanConfig(tableName: String,
                        statements: Seq[Select],
                        ecql: Option[Filter],
                        toFeatures: (Iterator[Row]) => Iterator[SimpleFeature])
}
