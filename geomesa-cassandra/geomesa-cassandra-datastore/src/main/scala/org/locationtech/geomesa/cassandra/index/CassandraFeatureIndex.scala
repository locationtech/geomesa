/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core._
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.index.legacy.{CassandraAttributeIndexV1, CassandraZ2IndexV1, CassandraZ3IndexV1}
import org.locationtech.geomesa.index.index.ClientSideFiltering
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType

object CassandraFeatureIndex extends CassandraIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[CassandraFeatureIndex] =
    Seq(CassandraZ3Index, CassandraZ3IndexV1, CassandraXZ3Index, CassandraZ2Index, CassandraZ2IndexV1,
      CassandraXZ2Index, CassandraIdIndex, CassandraAttributeIndex, CassandraAttributeIndexV1)

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices

  implicit class RichByteArray(val array: Array[Byte]) extends AnyVal {
    def getOrElse(i: Int, default: Byte): Byte = if (array.length > i) { array(i) } else { default }
  }
}

trait CassandraFeatureIndex extends CassandraFeatureIndexType with ClientSideFiltering[Row] with LazyLogging {

  protected val sfts = new ThreadLocal[SimpleFeatureType]

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
      logger.info(create)
      ds.session.execute(create)
    }
  }

  override def removeAll(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    val tableName = getTableName(sft.getTypeName, ds)
    val truncate = s"TRUNCATE $tableName"
    logger.info(truncate)
    ds.session.execute(truncate)
  }

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = {
    if (shared) {
      throw new NotImplementedError("Cassandra tables are never shared")
    } else {
      val tableName = getTableName(sft.getTypeName, ds)
      val delete = s"DROP TABLE IF EXISTS $tableName"
      logger.info(delete)
      ds.session.execute(delete)
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

  override def rowAndValue(result: Row): RowAndValue = {
    val values = columns.map(c => RowValue(c, result.get(c.i, c.jType).asInstanceOf[AnyRef]))
    val sf = result.getBytes("sf")

    val row = columnsToRow(values)
    val value = Array.ofDim[Byte](sf.limit())
    sf.get(value)

    RowAndValue(row, 0, row.length, value, 0, value.length)
  }
}
