/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.regex.Pattern

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

abstract class SpatialScheme(bits: Int, geom: String) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val format = s"%0${digits(bits)}d"

  protected def digits(bits: Int): Int

  protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  override val depth: Int = 1

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    val geometries = FilterHelper.extractGeometries(filter, geom, intersect = true)
    if (geometries.disjoint) {
      Some(Seq.empty)
    } else if (geometries.values.isEmpty) {
      None
    } else {
      val partitions = partition.map(Seq(_)).getOrElse {
        // there should be few enough partitions that we can enumerate them here and not exactly match the filter...
        val ranges = generateRanges(geometries.values.map(GeometryUtils.bounds))
        ranges.flatMap(r => r.lower to r.upper).distinct.map(_.formatted(format))
      }
      // note: we don't simplify the filter as usually we wouldn't be able to remove much
      Some(Seq(SimplifiedFilter(filter, partitions, partial = false)))
    }
  }
}

object SpatialScheme {

  object Config {
    val GeomAttribute: String = "geom-attribute"
    val Z2Resolution : String = s"${Z2Scheme.Name}-resolution"
    val XZ2Resolution: String = s"${XZ2Scheme.Name}-resolution"
  }

  abstract class SpatialPartitionSchemeFactory(name: String) extends PartitionSchemeFactory {

    private val namePattern: Pattern = Pattern.compile(s"$name(-([0-9]+)bits?)?")
    private val resolution = s"$name-resolution"

    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val matcher = namePattern.matcher(config.name)
      if (!matcher.matches()) { None } else {
        val geom = config.options.getOrElse(Config.GeomAttribute, sft.getGeomField)
        val geomIndex = sft.indexOf(geom)
        if (geomIndex == -1) {
          throw new IllegalArgumentException(s"$name scheme requires valid geometry field '${Config.GeomAttribute}'")
        }
        val res =
          Option(matcher.group(2))
              .filterNot(_.isEmpty)
              .orElse(config.options.get(resolution))
              .map(Integer.parseInt)
              .getOrElse(throw new IllegalArgumentException(s"$name scheme requires bit resolution '$resolution'"))
        Some(buildPartitionScheme(res, geom, geomIndex))
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme
  }
}
