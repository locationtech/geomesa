/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Value, Range => aRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToMutations
import org.locationtech.geomesa.accumulo.data.{EMPTY_TEXT, WritableFeature}
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.sfcurve.zorder.{Z3, ZPrefix}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object Z3Table extends GeoMesaTable {

  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  // the bytes of z we keep for complex geoms
  // 3 bytes is 15 bits of geometry (not including time bits and the first 2 bits which aren't used)
  // roughly equivalent to 3 digits of geohash (32^3 == 2^15) and ~78km resolution
  // (4 bytes is 20 bits, equivalent to 4 digits of geohash and ~20km resolution)
  // note: we also lose time resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getDtgField.isDefined && ((sft.getSchemaVersion > 6 && sft.getGeometryDescriptor != null) ||
        (sft.getSchemaVersion > 4 && sft.isPoints))

  override val suffix: String = "z3"

  // z3 always needs a separate table since we don't include the feature name in the row key
  override def formatTableName(prefix: String, sft: SimpleFeatureType): String =
    GeoMesaTable.formatSoloTableName(prefix, suffix, sft.getTypeName)

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val getRowKeys: (WritableFeature, Int) => Seq[Array[Byte]] = {
      if (sft.isPoints) {
        if (hasSplits(sft)) {
          getPointRowKey(timeToIndex, sfc)
        } else {
          (wf, i) => getPointRowKey(timeToIndex, sfc)(wf, i).map(_.drop(1))
        }
      } else {
        getGeomRowKeys(timeToIndex, sfc)
      }
    }
    if (sft.getSchemaVersion < 9) {
      (wf: WritableFeature) => {
        val rows = getRowKeys(wf, dtgIndex)
        // store the duplication factor in the column qualifier for later use
        val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
        rows.map { row =>
          val mutation = new Mutation(row)
          wf.fullValues.foreach(value => mutation.put(FULL_CF, cq, value.vis, value.value))
          wf.binValues.foreach(value => mutation.put(BIN_CF, cq, value.vis, value.value))
          mutation
        }
      }
    } else {
      (wf: WritableFeature) => {
        val rows = getRowKeys(wf, dtgIndex)
        // store the duplication factor in the column qualifier for later use
        val duplication = Integer.toHexString(rows.length)
        rows.map { row =>
          val mutation = new Mutation(row)
          wf.fullValues.foreach { value =>
            val cq = new Text(s"$duplication,${value.cq.toString}")
            mutation.put(value.cf, cq, value.vis, value.value)
          }
          wf.binValues.foreach { value =>
            val cq = new Text(s"$duplication,${value.cq.toString}")
            mutation.put(value.cf, cq, value.vis, value.value)
          }
          mutation
        }
      }
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val getRowKeys: (WritableFeature, Int) => Seq[Array[Byte]] = {
      if (sft.isPoints) {
        if (hasSplits(sft)) {
          getPointRowKey(timeToIndex, sfc)
        } else {
          (ftw, i) => getPointRowKey(timeToIndex, sfc)(ftw, i).map(_.drop(1))
        }
      } else {
        getGeomRowKeys(timeToIndex, sfc)
      }
    }
    if (sft.getSchemaVersion < 9) {
      (wf: WritableFeature) => {
        val rows = getRowKeys(wf, dtgIndex)
        val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
        rows.map { row =>
          val mutation = new Mutation(row)
          wf.fullValues.foreach(value => mutation.putDelete(FULL_CF, cq, value.vis))
          wf.binValues.foreach(value => mutation.putDelete(BIN_CF, cq, value.vis))
          mutation
        }
      }
    } else {
      (wf: WritableFeature) => {
        val rows = getRowKeys(wf, dtgIndex)
        val duplication = Integer.toHexString(rows.length)
        rows.map { row =>
          val mutation = new Mutation(row)
          wf.fullValues.foreach { value =>
            val cq = new Text(s"$duplication,${value.cq.toString}")
            mutation.putDelete(value.cf, cq, value.vis)
          }
          wf.binValues.foreach { value =>
            val cq = new Text(s"$duplication,${value.cq.toString}")
            mutation.putDelete(value.cf, cq, value.vis)
          }
          mutation
        }
      }
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = {
    val offset = getIdRowOffset(sft)
    (row: Text) => new String(row.getBytes, offset, row.getLength - offset, StandardCharsets.UTF_8)
  }

  override def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    bd.setRanges(Seq(new aRange()))
    bd.delete()
  }

  // geoms always have splits, but they weren't added until schema 7
  def hasSplits(sft: SimpleFeatureType) = sft.getSchemaVersion > 6

  // split(1 byte), week(2 bytes), z value (8 bytes), id (n bytes)
  private def getPointRowKey(timeToIndex: TimeToBinnedTime, sfc: Z3SFC)
                            (wf: WritableFeature, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(wf.idHash % NUM_SPLITS)
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
  private def getGeomRowKeys(timeToIndex: TimeToBinnedTime, sfc: Z3SFC)
                            (wf: WritableFeature, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(wf.idHash % NUM_SPLITS)
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
        zBox(sfc, xmin, ymin, xmax, ymax, t)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox(sfc, _, t))
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      zBox(sfc, env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, t)
  }

  // gets a sequence of (week, z) values that cover the bounding box
  private def zBox(sfc: Z3SFC, xmin: Double, ymin: Double, xmax: Double, ymax: Double, t: Long): Set[Long] = {
    val zmin = sfc.index(xmin, ymin, t).z
    val zmax = sfc.index(xmax, ymax, t).z
    getZPrefixes(zmin, zmax)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the interval
  private def getZPrefixes(zmin: Long, zmax: Long): Set[Long] = {
    val in = scala.collection.mutable.Queue((zmin, zmax))
    val out = scala.collection.mutable.HashSet.empty[Long]

    while (in.nonEmpty) {
      val (min, max) = in.dequeue()
      val ZPrefix(zprefix, zbits) = Z3.longestCommonPrefix(min, max)
      if (zbits < GEOM_Z_NUM_BYTES * 8) {
        // divide the range into two smaller ones using tropf litmax/bigmin
        val (litmax, bigmin) = Z3.zdivide((min + max) >>> 1, min, max) // >>> 1 is overflow safe mean
        in.enqueue((min, litmax), (bigmin, max))
      } else {
        // we've found a prefix that contains our z range
        // truncate down to the bytes we use so we don't get dupes
        out.add(zprefix & GEOM_Z_MASK)
      }
    }

    out.toSet
  }

  // gets the offset into the row for the id bytes
  def getIdRowOffset(sft: SimpleFeatureType): Int = {
    val length = if (sft.isPoints) 10 else 2 + GEOM_Z_NUM_BYTES // week + z bytes
    val prefix = if (hasSplits(sft)) 1 else 0 // shard
    prefix + length
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val localityGroups = Seq(BIN_CF, FULL_CF).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = SPLIT_ARRAYS.drop(1).map(new Text(_)).toSet
    val splitsToAdd = splits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
