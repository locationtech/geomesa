/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.conf.Property
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait Z3WritableIndex extends AccumuloWritableIndex {

  import AccumuloWritableIndex.{BinColumnFamily, FullColumnFamily}
  import Z3Index.{GEOM_Z_MASK, GEOM_Z_NUM_BYTES, GEOM_Z_STEP}

  def hasSplits: Boolean

  override def removeAll(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    val table = ops.getTableName(sft.getTypeName, this)
    if (ops.tableOps.exists(table)) {
      ops.tableOps.delete(table)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = {
    val offset = getIdRowOffset(sft)
    (row: Text) => new String(row.getBytes, offset, row.getLength - offset, StandardCharsets.UTF_8)
  }

  // split(1 byte), week(2 bytes), z value (8 bytes), id (n bytes)
  protected def getPointRowKey(timeToIndex: TimeToBinnedTime, sfc: Z3SFC)
                              (wf: WritableFeature, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = AccumuloWritableIndex.DefaultSplitArrays(wf.idHash % AccumuloWritableIndex.DefaultNumSplits)
    val (timeBin, z) = {
      val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) 0 else dtg.getTime
      val BinnedTime(b, t) = timeToIndex(time)
      val geom = wf.feature.point
      (b, sfc.index(geom.getX, geom.getY, t).z)
    }
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    Seq(Bytes.concat(split, Shorts.toByteArray(timeBin), Longs.toByteArray(z), id))
  }

  // split(1 byte), week (2 bytes), z value (3 bytes), id (n bytes)
  protected def getGeomRowKeys(timeToIndex: TimeToBinnedTime, sfc: Z3SFC)
                              (wf: WritableFeature, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = AccumuloWritableIndex.DefaultSplitArrays(wf.idHash % AccumuloWritableIndex.DefaultNumSplits)
    val (timeBin, zs) = {
      val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) 0 else dtg.getTime
      val BinnedTime(b, t) = timeToIndex(time)
      val geom = wf.feature.getDefaultGeometry.asInstanceOf[Geometry]
      (Shorts.toByteArray(b), zBox(sfc, geom, t).toSeq)
    }
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    zs.map(z => Bytes.concat(split, timeBin, Longs.toByteArray(z).take(GEOM_Z_NUM_BYTES), id))
  }

  // gets a sequence of (week, z) values that cover the geometry
  private def zBox(sfc: Z3SFC, geom: Geometry, t: Long): Set[Long] = geom match {
    case g: Point => Set(sfc.index(g.getX, g.getY, t).z)
    case g: LineString =>
      // we flatMap bounds for each line segment so we cover a smaller area
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        getZPrefixes(sfc, xmin, ymin, xmax, ymax, t)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox(sfc, _, t))
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      getZPrefixes(sfc, env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, t)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the bounding box
  private def getZPrefixes(sfc: Z3SFC, xmin: Double, ymin: Double, xmax: Double, ymax: Double, t: Long): Set[Long] = {
    sfc.ranges((xmin, xmax), (ymin, ymax), (t, t), 8 * GEOM_Z_NUM_BYTES).flatMap { range =>
      val lower = range.lower & GEOM_Z_MASK
      val upper = range.upper & GEOM_Z_MASK
      if (lower == upper) {
        Seq(lower)
      } else {
        val count = ((upper - lower) / GEOM_Z_STEP).toInt
        Seq.tabulate(count)(i => lower + i * GEOM_Z_STEP) :+ upper
      }
    }.toSet
  }

  // gets the offset into the row for the id bytes
  def getIdRowOffset(sft: SimpleFeatureType): Int = {
    val length = if (sft.isPoints) 10 else 2 + GEOM_Z_NUM_BYTES // week + z bytes
    val prefix = if (hasSplits) 1 else 0 // shard
    prefix + length
  }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
      // z3 always has it's own table
    val table = GeoMesaTable.formatSoloTableName(ops.catalogTable, tableSuffix, sft.getTypeName)
    ops.metadata.insert(sft.getTypeName, tableNameKey, table)

    AccumuloVersion.ensureTableExists(ops.connector, table)

    val localityGroups = Seq(BinColumnFamily, FullColumnFamily).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    ops.tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = AccumuloWritableIndex.DefaultSplitArrays.drop(1).map(new Text(_)).toSet
    val splitsToAdd = splits -- ops.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ops.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    ops.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}