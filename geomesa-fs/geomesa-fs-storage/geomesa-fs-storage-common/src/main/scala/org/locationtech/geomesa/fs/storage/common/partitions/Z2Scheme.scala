/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.regex.Pattern
import java.util.{Collections, Optional}

import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialPartitionSchemeConfig._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class Z2Scheme(bits: Int, geom: String, leaf: Boolean) extends SpatialScheme(bits, geom, leaf) {
  override protected val digits: Int = math.ceil(bits * math.log10(2)).toInt

  override def getName: String = Z2Scheme.Name

  private val z2 = new Z2SFC(bits / 2)

  override def getPartition(feature: SimpleFeature): String = {
    val pt = feature.getAttribute(geom).asInstanceOf[Point]
    z2.index(pt.getX, pt.getY).z.formatted(s"%0${digits}d")
  }

  override def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[Long] =
    z2.ranges(xy).flatMap(ir => ir.lower to ir.upper)
}

class XZ2Scheme(bits: Int, geom: String, leaf: Boolean) extends SpatialScheme(bits, geom, leaf) {
  // The max XZ2 value is (4^((bits/2)+1)-1)/3.  This calculates the number of digits in that value.
  override protected val digits: Int = math.ceil(((bits/2)+1)* math.log10(4) - math.log10(3)).toInt

  override def getName: String = XZ2Scheme.Name

  private val xz2 = XZ2SFC((bits / 2).asInstanceOf[Short])

  override def getPartition(feature: SimpleFeature): String = {
    val geometry = feature.getAttribute(geom).asInstanceOf[Geometry]
    val envelope = geometry.getEnvelopeInternal
    xz2.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY).formatted(s"%0${digits}d")
  }

  override def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[Long] =
    xz2.ranges(xy).flatMap(ir => ir.lower to ir.upper)
}

abstract class SpatialScheme(bits: Int, geom: String, leaf: Boolean) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val digits: Int // = math.ceil(math.log10(math.pow(2, bits))).toInt

  val resolutionFieldName: String = s"$getName-resolution"

  def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[Long]

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
      val xy: Seq[(Double, Double, Double, Double)] = geometries.values.map(GeometryUtils.bounds)
      val enumerations: Seq[Long] = generateRanges(xy)
      enumerations.map(_.formatted(s"%0${digits}d")).asJava
    }
  }

  override def getOptions: java.util.Map[String, String] = {
    import scala.collection.JavaConverters._
    Map(
      GeomAttribute        -> geom,
      resolutionFieldName  -> bits.toString,
      LeafStorage          -> leaf.toString
    ).asJava
  }

  override def getMaxDepth: Int = 1

  override def isLeafStorage: Boolean = leaf

  override def equals(other: Any): Boolean = other match {
    case that: Z2Scheme => that.getOptions.equals(getOptions)
    case _ => false
  }

  override def hashCode(): Int = getOptions.hashCode()
}

object Z2Scheme {
  val Name = "z2"
  class Z2PartitionSchemeFactory extends SpatialPartitionSchemeFactory {
    override val Name: String = Z2Scheme.Name

    override def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme =
      new Z2Scheme(bits, geom, leaf)
  }
}

object XZ2Scheme {
  val Name: String = "xz2"
  class XZ2PartitionSchemeFactory extends SpatialPartitionSchemeFactory {
    override val Name: String = XZ2Scheme.Name

    override def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme =
      new XZ2Scheme(bits, geom, leaf)
  }
}

object SpatialPartitionSchemeConfig {
  val GeomAttribute: String = "geom-attribute"
  val LeafStorage: String = "leaf-storage"
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
      val geom = Option(options.get(GeomAttribute)).getOrElse(sft.getGeomField)
      if (sft.indexOf(geom) == -1) {
        throw new IllegalArgumentException(s"$Name scheme requires valid geometry field '${GeomAttribute}'")
      }
      val res =
        Option(matcher.group(2))
          .filterNot(_.isEmpty)
          .orElse(Option(options.get(Resolution)))
          .map(Integer.parseInt)
          .getOrElse {
            throw new IllegalArgumentException(s"$Name scheme requires bit resolution '${Resolution}'")
          }
      val leaf = Option(options.get(LeafStorage)).forall(java.lang.Boolean.parseBoolean)
      Optional.of(buildPartitionScheme(res, geom, leaf))
    }
  }

  def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme
}
