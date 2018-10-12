/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import java.nio.charset.StandardCharsets

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, LineString, Point}
import org.apache.accumulo.core.conf.Property
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{AccumuloColumnGroups, AccumuloFeatureIndex}
import org.locationtech.geomesa.curve.LegacyZ2SFC
import org.locationtech.geomesa.index.utils.SplitArrays
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait Z2WritableIndex extends AccumuloFeatureIndex {

  import AccumuloColumnGroups.BinColumnFamily
  import org.locationtech.geomesa.accumulo.index.legacy.z2.Z2IndexV1._

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String = {
    val start = getIdRowOffset(sft)
    (row, offset, length, feature) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  // split(1 byte), z value (8 bytes), id (n bytes)
  protected def getPointRowKey(tableSharing: Array[Byte], splitArray: Seq[Array[Byte]], lenient: Boolean)
                              (wf: AccumuloFeature): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val numSplits = splitArray.length
    val split = splitArray(wf.idHash % numSplits)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    val geom = wf.feature.point
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${wf.feature.getID}")
    }
    val z = LegacyZ2SFC.index(geom.getX, geom.getY, lenient).z
    Seq(Bytes.concat(tableSharing, split, Longs.toByteArray(z), id))
  }

  // split(1 byte), z value (3 bytes), id (n bytes)
  protected def getGeomRowKeys(tableSharing: Array[Byte], splitArray: Seq[Array[Byte]])
                              (wf: AccumuloFeature): Seq[Array[Byte]] = {
    val numSplits = splitArray.length
    val split = splitArray(wf.idHash % numSplits)
    val geom = wf.feature.getDefaultGeometry.asInstanceOf[Geometry]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${wf.feature.getID}")
    }
    val zs = zBox(geom)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    zs.map(z => Bytes.concat(tableSharing, split, Longs.toByteArray(z).take(GEOM_Z_NUM_BYTES), id)).toSeq
  }

  // gets a sequence of z values that cover the geometry
  private def zBox(geom: Geometry): Set[Long] = geom match {
    case g: Point => Set(LegacyZ2SFC.index(g.getX, g.getY).z)
    case g: LineString =>
      // we flatMap bounds for each line segment so we cover a smaller area
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        getZPrefixes(xmin, ymin, xmax, ymax)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox)
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      getZPrefixes(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the bounding box
  private def getZPrefixes(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Set[Long] = {
    LegacyZ2SFC.ranges((xmin, xmax), (ymin, ymax), 8 * GEOM_Z_NUM_BYTES).flatMap { range =>
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

  protected def sharingPrefix(sft: SimpleFeatureType): Array[Byte] = {
    val sharing = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    sharing
  }

  // gets the offset into the row for the id bytes
  def getIdRowOffset(sft: SimpleFeatureType): Int = {
    val length = if (sft.isPoints) 8 else GEOM_Z_NUM_BYTES
    val prefix = if (sft.isTableSharing) 2 else 1 // shard + table sharing
    prefix + length
  }

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): String = {
    import scala.collection.JavaConversions._

    val table = super.configure(sft, ds, partition)

    AccumuloVersion.ensureTableExists(ds.connector, table)

    val localityGroups = Seq(BinColumnFamily, AccumuloColumnGroups.default).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    ds.tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = if (sft.isTableSharing) {
      val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
      SplitArrays.apply(sft.getZShards).drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      SplitArrays.apply(sft.getZShards).drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- ds.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ds.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    table
  }
}