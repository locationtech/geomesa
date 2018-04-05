/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.{Collections, Optional}
import java.util.regex.Pattern

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class Z2Scheme(bits: Int, geom: String, leaf: Boolean) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  // note: z2sfc resolution is per dimension
  private val z2 = new Z2SFC(bits / 2)
  private val digits = math.ceil(math.log10(math.pow(2, bits))).toInt

  override def getName: String = Z2Scheme.Name

  override def getPartition(feature: SimpleFeature): String = {
    // TODO support non-point geoms
    val pt = feature.getAttribute(geom).asInstanceOf[Point]
    z2.index(pt.getX, pt.getY).z.formatted(s"%0${digits}d")
  }

  override def getPartitions(filter: Filter): java.util.List[String] = {
    import scala.collection.JavaConverters._

    val geometries: FilterValues[Geometry] = {
      val extracted = FilterHelper.extractGeometries(filter, geom, intersect = true)
      if (extracted.nonEmpty) {
        extracted
      } else {
        FilterValues(Seq(WholeWorldPolygon))
      }
    }

    if (geometries.disjoint) {
      Collections.emptyList()
    } else {
      val xy = geometries.values.map(GeometryUtils.bounds)
      val enumerations = z2.ranges(xy).flatMap(ir => ir.lower to ir.upper)
      enumerations.map(_.formatted(s"%0${digits}d")).asJava
    }
  }

  override def getMaxDepth: Int = 1

  override def isLeafStorage: Boolean = leaf

  override def getOptions: java.util.Map[String, String] = {
    import Z2Scheme.Config.{GeomAttribute, LeafStorage, Z2Resolution}

    import scala.collection.JavaConverters._

    Map(
      GeomAttribute -> geom,
      Z2Resolution  -> bits.toString,
      LeafStorage   -> leaf.toString
    ).asJava
  }

  override def equals(other: Any): Boolean = other match {
    case that: Z2Scheme => that.getOptions.equals(getOptions)
    case _ => false
  }

  override def hashCode(): Int = getOptions.hashCode()
}

object Z2Scheme {

  val Name = "z2"

  private val NamePattern = Pattern.compile(s"$Name(-([0-9]+)bits?)?")

  object Config {
    val GeomAttribute = "geom-attribute"
    val Z2Resolution  = "z2-resolution"
    val LeafStorage   = "leaf-storage"
  }

  class Z2PartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val matcher = NamePattern.matcher(name)
      if (!matcher.matches()) { Optional.empty() } else {
        val geom = Option(options.get(Config.GeomAttribute)).getOrElse(sft.getGeomField)
        if (sft.indexOf(geom) == -1) {
          throw new IllegalArgumentException(s"Z2 scheme requires valid geometry field '${Config.GeomAttribute}'")
        }
        val res =
          Option(matcher.group(2))
            .filterNot(_.isEmpty)
            .orElse(Option(options.get(Config.Z2Resolution)))
            .map(Integer.parseInt)
            .getOrElse {
              throw new IllegalArgumentException(s"Z2 scheme requires bit resolution '${Config.Z2Resolution}'")
            }
        val leaf = Option(options.get(Config.LeafStorage)).forall(java.lang.Boolean.parseBoolean)
        Optional.of(new Z2Scheme(res, geom, leaf))
      }
    }
  }
}