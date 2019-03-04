/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.regex.Pattern
import java.util.{Collections, Optional}

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.{FilterPartitions, PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.Config
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

abstract class SpatialScheme(bits: Int, geom: String, leaf: Boolean) extends PartitionScheme {

  import scala.collection.JavaConverters._

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val format = s"%0${digits(bits)}d"

  protected def digits(bits: Int): Int

  protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  override def getFilterPartitions(filter: Filter): Optional[java.util.List[FilterPartitions]] = {
    val geometries = FilterHelper.extractGeometries(filter, geom, intersect = true)
    if (geometries.disjoint) {
      Optional.of(Collections.emptyList())
    } else {
      // there should be few enough partitions that we can enumerate them here and not exactly match the filter
      // we don't simplify the filter as usually we wouldn't be able to remove much
      val bounds = if (geometries.values.isEmpty) { Seq(WholeWorldPolygon) } else { geometries.values }
      val ranges = generateRanges(bounds.map(GeometryUtils.bounds))
      val partitions = ranges.flatMap(r => r.lower to r.upper).distinct.map(_.formatted(format))
      Optional.of(Collections.singletonList(new FilterPartitions(filter, partitions.asJava, false)))
    }
  }

  override def getOptions: java.util.Map[String, String] = {
    Map(
      Config.GeomAttribute          -> geom,
      Config.resolutionOption(this) -> bits.toString,
      Config.LeafStorage            -> leaf.toString
    ).asJava
  }

  override def getMaxDepth: Int = 1

  override def isLeafStorage: Boolean = leaf

  override def equals(other: Any): Boolean =
    getClass == other.getClass && other.asInstanceOf[SpatialScheme].getOptions == getOptions

  override def hashCode(): Int = getOptions.hashCode()
}

object SpatialScheme {

  object Config {
    val GeomAttribute: String = "geom-attribute"
    val LeafStorage  : String = LeafStorageConfig

    def resolutionOption(scheme: SpatialScheme): String = s"${scheme.getName}-resolution"
  }

  trait SpatialPartitionSchemeFactory extends PartitionSchemeFactory {

    def Name: String

    lazy val NamePattern: Pattern = Pattern.compile(s"$Name(-([0-9]+)bits?)?")
    lazy val Resolution = s"$Name-resolution"

    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val matcher = NamePattern.matcher(name)
      if (!matcher.matches()) { Optional.empty() } else {
        val geom = Option(options.get(Config.GeomAttribute)).getOrElse(sft.getGeomField)
        if (sft.indexOf(geom) == -1) {
          throw new IllegalArgumentException(s"$Name scheme requires valid geometry field '${Config.GeomAttribute}'")
        }
        val res =
          Option(matcher.group(2))
              .filterNot(_.isEmpty)
              .orElse(Option(options.get(Resolution)))
              .map(Integer.parseInt)
              .getOrElse(throw new IllegalArgumentException(s"$Name scheme requires bit resolution '$Resolution'"))
        val leaf = Option(options.get(Config.LeafStorage)).forall(java.lang.Boolean.parseBoolean)
        Optional.of(buildPartitionScheme(res, geom, leaf))
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme
  }
}
