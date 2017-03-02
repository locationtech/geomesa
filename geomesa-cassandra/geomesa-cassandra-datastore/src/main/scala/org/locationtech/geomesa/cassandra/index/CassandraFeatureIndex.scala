/***********************************************************************
* Copyright (c) 2013-2016 IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.google.common.primitives.{Bytes, Longs, Ints, Shorts}

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder._
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.curve.TimePeriod
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Date

object CassandraFeatureIndex extends CassandraIndexManagerType {
  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[CassandraFeatureIndex] =
    Seq(CassandraZ3Index, CassandraXZ3Index, CassandraZ2Index, CassandraXZ2Index, CassandraIdIndex, CassandraAttributeIndex)
  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices
}

trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, CassandraRow, CassandraRow] with ClientSideFiltering[Row] {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  type RowToValues = (Array[Byte]) => Any

  def getRowValuesFromSFT(sft: SimpleFeatureType): RowToValues = {
    // table sharing + shard + 2 byte short + 8 byte long + fid
    val sharing = sft.getTableSharingPrefix
    (row) => {
      var curr = if (sharing == "") 0 else 1
      // use shard and bin for pk
      val pk:java.lang.Integer = ((row(curr) & 0xFF) << 16) | ((row(curr + 1) & 0xFF) << 8) | (row(curr + 2) & 0xFF);
      curr += 3
      val z = ByteBuffer.wrap(row, curr, 8).getLong
      curr += 8
      val fid = new String(row, curr, row.length - curr, StandardCharsets.UTF_8)
      (pk, z, fid, sharing)
    }
  }

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    super.configure(sft, ds)
    val tableName = getTableName(sft.getTypeName, ds)
    val cluster = ds.session.getCluster
    val ks = ds.session.getLoggedKeyspace
    val table = cluster.getMetadata().getKeyspace(ks).getTable(tableName);
    if (table == null){
      ds.session.execute(s"CREATE TABLE $tableName (pk int, sharing text, z bigint, fid text, sf blob, PRIMARY KEY (pk, sharing, z, fid))")
    }
  }

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = {
    val tableName = getTableName(sft.getTypeName, ds)
    ds.session.execute(s"drop table $tableName")
  }

  override protected def createInsert(row: Array[Byte], cf: CassandraFeature): CassandraRow = {
    val (pk:java.lang.Integer, z:java.lang.Long, fid:java.lang.String, sharing:java.lang.String) = getRowValuesFromSFT(cf.feature.getFeatureType)(row)
    val qs = "INSERT INTO %s (pk, z, fid, sharing, sf) VALUES (?, ?, ?, ?, ?)"
    CassandraRow(qs=Some(qs), values=Some(Seq(pk, z, fid, sharing, ByteBuffer.wrap(cf.fullValue))))
  }

  override protected def createDelete(row: Array[Byte], cf: CassandraFeature): CassandraRow = {
    val (pk:java.lang.Integer, z:java.lang.Long, sharing:java.lang.String, fid:java.lang.String) = getRowValuesFromSFT(cf.feature.getFeatureType)(row)
    val qs = "DELETE FROM %s WHERE pk=$pk AND fid=$fid AND sharing=$sharing"
    CassandraRow(qs=Some(qs))
  }

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: FilterStrategy[CassandraDataStore, CassandraFeature, CassandraRow],
                                  hints: Hints,
                                  ranges: Seq[CassandraRow],
                                  ecql: Option[Filter]): CassandraQueryPlanType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    if (ranges.isEmpty) {
      EmptyPlan(filter)
    } else {
      val ks = ds.session.getLoggedKeyspace
      val tableName = getTableName(sft.getTypeName, ds)
      val getRowValues = getRowValuesFromSFT(sft)

      val queries = ranges.map(row => {
        val select = QueryBuilder.select.all.from(ks, tableName)
        val start = row.start.get
        val end = row.end.getOrElse(Array())

        if ((start.length > 0) && (end.length > 0)){
          val (stPk, stZ, stFid, stSharing) = getRowValues(start)
          val (edPk, edZ, edFid, _) = getRowValues(end)

          val where = if (stPk == edPk) {
            select.where(QueryBuilder.eq("pk", stPk))
          } else {
            select.where(QueryBuilder.in("pk", java.util.Arrays.asList(stPk, edPk)))
          }

          where.and(QueryBuilder.eq("sharing", stSharing))
            .and(QueryBuilder.gt("z", stZ))
            .and(QueryBuilder.lt("z", edZ))

          if (stFid.asInstanceOf[java.lang.String].length > 0){
            where.and(QueryBuilder.gte("fid", stFid))
              .and(QueryBuilder.lte("fid", edFid))
          }
        } else if (start.length > 0){
          val (stPk, stZ, stSharing, stFid, z3Interval) = getRowValues(start)

          val where = select.where(QueryBuilder.eq("pk", stPk))
            .and(QueryBuilder.eq("sharing", stSharing))
            .and(QueryBuilder.gte("z", stZ))

          if (stFid.asInstanceOf[java.lang.String].length > 0){
            where.and(QueryBuilder.gte("fid", stFid))
          }
        } else if (end.length > 0){
          val (edPk, edZ, edSharing, edFid, z3Interval) = getRowValues(end)

          val where = select.where(QueryBuilder.eq("pk", edPk))
            .and(QueryBuilder.eq("sharing", edSharing))
            .and(QueryBuilder.lte("z", edZ))

          if (edFid.asInstanceOf[java.lang.String].length > 0){
            where.and(QueryBuilder.lte("fid", edFid))
          }

        }
        CassandraRow(stmt=Some(select))
      })
      val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
      QueryPlan(filter, tableName, queries, ecql, toFeatures)
    }
  }

  override protected def range(start:Array[Byte], end: Array[Byte]):CassandraRow = {
    CassandraRow(start=Some(start), end=Some(end))
  }

  override protected def rangeExact(row: Array[Byte]): CassandraRow = {
    CassandraRow(start=Some(row))
  }

  override protected def rowAndValue(result: Row): RowAndValue = {
    val pk = Ints.toByteArray(result.getInt(0)) // 0, shard, bin (2 bytes)
    val sharing = result.getString(1)
    val z = result.getLong(2)
    val fid:Array[Byte] = result.getString(3).getBytes
    val sf:Array[Byte] = result.getBytes(4).array

    val row = if (sharing == "") {
      Bytes.concat(Array(pk(1)), Array(pk(2), pk(3)), Longs.toByteArray(z), fid)
    } else {
      Bytes.concat(sharing.getBytes, Array(pk(1)), Array(pk(2), pk(3)), Longs.toByteArray(z), fid)
    }
    RowAndValue(row, 0, row.length, sf, 0, sf.length)
  }
}
