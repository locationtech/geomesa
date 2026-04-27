/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.zorder.sfcurve.IndexRange
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern
import scala.reflect.ClassTag

abstract class SpatialScheme(id: String, bits: Int, geom: String) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val format = s"%0${digits(bits)}d"

  override val name: String = s"$id:attribute=$geom:bits=$bits"

  protected def digits(bits: Int): Int

  protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getGeoms(filter).map { ranges =>
      val builder = new RangeBuilder()
      ranges.foreach { range =>
        val lower = format.format(range.lower)
        val upper = format.format(range.upper + 1)
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getGeoms(filter).orElse(Some(generateRanges(Seq((-180, -90, 180, 90))))).map { ranges =>
      ranges.flatMap { range =>
        val lower = range.lower
        val steps = 1 + (range.upper - lower).toInt
        Seq.tabulate(steps)(i => PartitionKey(name, format.format(lower + i)))
      }
    }
  }

  private def getGeoms(filter: Filter): Option[Seq[IndexRange]] = {
    val geometries = FilterHelper.extractGeometries(filter, geom, intersect = true)
    if (geometries.isEmpty) {
      None
    } else if (geometries.disjoint) {
      Some(Seq.empty)
    } else {
      Some(generateRanges(geometries.values.map(GeometryUtils.bounds)))
    }
  }
}

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  abstract class SpatialPartitionSchemeFactory[T <: Geometry : ClassTag](val name: String) extends PartitionSchemeFactory {

    private val namePattern: Pattern = Pattern.compile(s"$name-([0-9]+)bits?:?")

    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      lazy val matcher = namePattern.matcher(scheme)

      def build(resolution: Short): Option[PartitionScheme] = {
        val geom = opts.getSingle("attribute").orElse(Option(sft.getGeomField)).orNull
        require(geom != null, s"Spatial schemes requires an attribute to be specified with 'attribute=<attribute>'")
        val index = attributeIndex(sft, geom, Some(implicitly[ClassTag[T]].runtimeClass))
        Some(buildPartitionScheme(resolution, geom, index))
      }

      if (opts.name == this.name) {
        val bits = opts.getSingle("bits").map(_.toShort).getOrElse {
          throw new IllegalArgumentException(s"Spatial schemes requires a resolution to be specified with 'bits=<resolution>'")
        }
        build(bits)
      } else if (matcher.matches()) {
        build(matcher.group(1).toShort)
      } else {
        None
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme
  }
}
