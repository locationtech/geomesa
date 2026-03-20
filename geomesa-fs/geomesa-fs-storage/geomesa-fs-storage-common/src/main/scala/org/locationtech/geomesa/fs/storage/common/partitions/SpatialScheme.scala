/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionFilter, PartitionRange, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.zorder.sfcurve.IndexRange

import java.util.regex.Pattern

abstract class SpatialScheme(id: String, bits: Int, geom: String) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val format = s"%0${digits(bits)}d"

  override val name: String = s"$id:attribute=$geom:bits=$bits"

  protected def digits(bits: Int): Int

  protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = {
    val geometries = FilterHelper.extractGeometries(filter, geom, intersect = true)
    if (geometries.isEmpty) {
      None
    } else if (geometries.disjoint) {
      Some(Seq.empty)
    } else {
      // there should be few enough partitions that we can safely enumerate them here
      val ranges = generateRanges(geometries.values.map(GeometryUtils.bounds)).map { range =>
        val lower = format.format(range.lower)
        if (lower == format.format(range.upper)) {
          SinglePartition(name, lower)
        } else {
          PartitionRange(name, lower, format.format(range.upper + 1))
        }
      }
      // TODO merge overlapping ranges
      // note: we don't simplify the filter as usually we wouldn't be able to remove much
      Some(Seq(PartitionFilter(ranges, Some(filter))))
    }
  }
}

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  abstract class SpatialPartitionSchemeFactory(name: String) extends PartitionSchemeFactory {

    private val namePattern: Pattern = Pattern.compile(s"$name-([0-9]+)bits?:?")

    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      lazy val matcher = namePattern.matcher(scheme)

      def build(resolution: Short): Option[PartitionScheme] = {
        val geom = opts.getSingle("attribute").orElse(Option(sft.getGeomField)).orNull
        require(geom != null, s"Spatial schemes requires an attribute to be specified with 'attribute=<attribute>'")
        val index = sft.indexOf(geom)
        require(index != -1, s"Attribute '$geom' does not exist in schema '${sft.getTypeName}'")
        Some(buildPartitionScheme(resolution, geom, index))
      }

      if (opts.name == this.name) {
        build(opts.getSingle("bits").map(_.toShort).getOrElse(2.toShort))
      } else if (matcher.matches()) {
        build(matcher.group(1).toShort)
      } else {
        None
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme
  }
}
