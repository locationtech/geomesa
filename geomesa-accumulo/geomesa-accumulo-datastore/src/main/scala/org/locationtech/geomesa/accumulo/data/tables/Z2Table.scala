/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables


import java.nio.charset.StandardCharsets

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, LineString, Point}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.sfcurve.zorder.Z2
import org.locationtech.sfcurve.zorder.Z3.ZPrefix
import org.opengis.feature.simple.SimpleFeatureType

object Z2Table extends GeoMesaTable {

  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  // the bytes of z we keep for complex geoms
  // 3 bytes is 22 bits of geometry (not including the first 2 bits which aren't used)
  // roughly equivalent to 4 digits of geohash (32^4 == 2^20) and ~20km resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long =
    java.lang.Long.decode("0x" + Array.fill(GEOM_Z_NUM_BYTES)("ff").mkString) << (8 - GEOM_Z_NUM_BYTES) * 8

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getGeometryDescriptor != null && sft.getSchemaVersion > 7

  override val suffix: String = "z2"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val sharing = sharingPrefix(sft)
    val getRowKeys: (FeatureToWrite) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing) else getGeomRowKeys(sharing)

    (fw: FeatureToWrite) => {
      val rows = getRowKeys(fw)
      // store the duplication factor in the column qualifier for later use
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        mutation.put(FULL_CF, cq, fw.columnVisibility, fw.dataValue)
        fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val sharing = sharingPrefix(sft)
    val getRowKeys: (FeatureToWrite) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing) else getGeomRowKeys(sharing)

    (fw: FeatureToWrite) => {
      val rows = getRowKeys(fw)
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        mutation.putDelete(BIN_CF, cq, fw.columnVisibility)
        mutation.putDelete(FULL_CF, cq, fw.columnVisibility)
        mutation
      }
    }
  }

  // split(1 byte), z value (8 bytes), id (n bytes)
  private def getPointRowKey(tableSharing: Array[Byte])(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val id = ftw.feature.getID.getBytes(StandardCharsets.UTF_8)
    val pt = ftw.feature.point
    val z = Z2SFC.index(pt.getX, pt.getY).z
    Seq(Bytes.concat(tableSharing, split, Longs.toByteArray(z), id))
  }

  // split(1 byte), z value (3 bytes), id (n bytes)
  private def getGeomRowKeys(tableSharing: Array[Byte])(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val geom = ftw.feature.getDefaultGeometry.asInstanceOf[Geometry]
    val zs = zBox(geom)
    val id = ftw.feature.getID.getBytes(StandardCharsets.UTF_8)
    zs.map(z => Bytes.concat(tableSharing, split, Longs.toByteArray(z).take(GEOM_Z_NUM_BYTES), id)).toSeq
  }

  // gets a sequence of z values that cover the geometry
  private def zBox(geom: Geometry): Set[Long] = geom match {
    case g: Point => Set(Z2SFC.index(g.getX, g.getY).z)
    case g: LineString =>
      // we flatMap bounds for each line segment so we cover a smaller area
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        zBox(xmin, ymin, xmax, ymax)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox)
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      zBox(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
  }

  // gets a sequence of z values that cover the bounding box
  private def zBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Set[Long] = {
    val zmin = Z2SFC.index(xmin, ymin).z
    val zmax = Z2SFC.index(xmax, ymax).z
    getZPrefixes(zmin, zmax)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the interval
  private def getZPrefixes(zmin: Long, zmax: Long): Set[Long] = {
    val in = scala.collection.mutable.Queue((zmin, zmax))
    val out = scala.collection.mutable.HashSet.empty[Long]

    while (in.nonEmpty) {
      val (min, max) = in.dequeue()
      val ZPrefix(zprefix, zbits) = Z2.longestCommonPrefix(min, max)
      if (zbits < GEOM_Z_NUM_BYTES * 8) {
        // divide the range into two smaller ones using tropf litmax/bigmin
        val (litmax, bigmin) = Z2.zdivide(Z2((min + max) / 2), Z2(min), Z2(max))
        in.enqueue((min, litmax.z), (bigmin.z, max))
      } else {
        // we've found a prefix that contains our z range
        // truncate down to the bytes we use so we don't get dupes
        out.add(zprefix & GEOM_Z_MASK)
      }
    }

    out.toSet
  }

  private def sharingPrefix(sft: SimpleFeatureType): Array[Byte] = {
    val sharing = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    sharing
  }

  // reads the feature ID from the row key
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    val length = if (sft.isPoints) 8 else GEOM_Z_NUM_BYTES
    val prefix = if (sft.isTableSharing) 2 else 1 // shard + table sharing
    val offset = prefix + length
    (row: Array[Byte]) => new String(row, offset, row.length - offset, StandardCharsets.UTF_8)
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val localityGroups = Seq(BIN_CF, FULL_CF).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = if (sft.isTableSharing) {
      val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
      SPLIT_ARRAYS.drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      SPLIT_ARRAYS.drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
