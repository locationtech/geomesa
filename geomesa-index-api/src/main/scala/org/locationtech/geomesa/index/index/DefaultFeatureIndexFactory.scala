/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.locationtech.geomesa.index.index.s2.S2Index
import org.locationtech.geomesa.index.index.s3.S3Index
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaFeatureIndexFactory}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.attribute.legacy._
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.id.legacy.{IdIndexV1, IdIndexV2, IdIndexV3}
import org.locationtech.geomesa.index.index.z2.legacy._
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.legacy._
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.utils.conf.IndexId
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

/**
  * Feature index factory providing the default indices shipped with geomesa. Note: this class is not SPI loaded
  */
object DefaultFeatureIndexFactory extends GeoMesaFeatureIndexFactory {

  private val available = Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, S3Index, S2Index, IdIndex, AttributeIndex)

  override def indices(sft: SimpleFeatureType, hint: Option[String]): Seq[IndexId] = {
    hint match {
      case None => available.flatMap(i => i.defaults(sft).map(IndexId(i.name, i.version, _)))
      case Some(h) => fromId(h).orElse(fromName(sft, h)).orElse(fromNameAndAttributes(sft, h)).getOrElse(Seq.empty)
    }
  }

  override def available(sft: SimpleFeatureType): Seq[(String, Int)] =
    available.collect { case i if i.defaults(sft).exists(i.supports(sft, _)) => (i.name, i.version) }

  override def create[T, U](ds: GeoMesaDataStore[_],
                            sft: SimpleFeatureType,
                            index: IndexId): Option[GeoMesaFeatureIndex[T, U]] = {

    import org.locationtech.geomesa.index.index.attribute.{AttributeIndex => AtIndex}

    lazy val Seq(geom3, dtg) = index.attributes
    lazy val Seq(geom2) = index.attributes
    lazy val Seq(attribute, secondary @ _*) = index.attributes

    val idx = (index.name, index.version) match {
      case (IdIndex.name, 4)  => Some(new IdIndex(ds, sft, index.mode))
      case (IdIndex.name, 3)  => Some(new IdIndexV3(ds, sft, index.mode))
      case (IdIndex.name, 2)  => Some(new IdIndexV2(ds, sft, index.mode))
      case (IdIndex.name, 1)  => Some(new IdIndexV1(ds, sft, index.mode))

      case (Z3Index.name, 6)  => Some(new Z3Index(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 5)  => Some(new Z3IndexV5(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 4)  => Some(new Z3IndexV4(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 3)  => Some(new Z3IndexV3(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 2)  => Some(new Z3IndexV2(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 1)  => Some(new Z3IndexV1(ds, sft, geom3, dtg, index.mode))

      case (XZ3Index.name, 2) => Some(new XZ3Index(ds, sft, geom3, dtg, index.mode))
      case (XZ3Index.name, 1) => Some(new XZ3IndexV1(ds, sft, geom3, dtg, index.mode))

      case (Z2Index.name, 5)  => Some(new Z2Index(ds, sft, geom2, index.mode))
      case (Z2Index.name, 4)  => Some(new Z2IndexV4(ds, sft, geom2, index.mode))
      case (Z2Index.name, 3)  => Some(new Z2IndexV3(ds, sft, geom2, index.mode))
      case (Z2Index.name, 2)  => Some(new Z2IndexV2(ds, sft, geom2, index.mode))
      case (Z2Index.name, 1)  => Some(new Z2IndexV1(ds, sft, geom2, index.mode))

      case (XZ2Index.name, 2) => Some(new XZ2Index(ds, sft, geom2, index.mode))
      case (XZ2Index.name, 1) => Some(new XZ2IndexV1(ds, sft, geom2, index.mode))

      case (S2Index.name, 1)  => Some(new S2Index(ds, sft, geom2, index.mode))
      case (S3Index.name, 1)  => Some(new S3Index(ds, sft, geom3, dtg, index.mode))

      case (AtIndex.name, 8)  => Some(new AttributeIndex(ds, sft, attribute, secondary, index.mode))
      case (AtIndex.name, 7)  => Some(new AttributeIndexV7(ds, sft, attribute, secondary, index.mode))
      case (AtIndex.name, 6)  => Some(new AttributeIndexV6(ds, sft, attribute, secondary, index.mode))
      case (AtIndex.name, 5)  => Some(new AttributeIndexV5(ds, sft, attribute, secondary, index.mode))
      case (AtIndex.name, 4)  => Some(new AttributeIndexV4(ds, sft, attribute, secondary, index.mode))
      case (AtIndex.name, 3)  => Some(new AttributeIndexV3(ds, sft, attribute, secondary.headOption, index.mode))
      case (AtIndex.name, 2)  => Some(new AttributeIndexV2(ds, sft, attribute, secondary.headOption, index.mode))

      case _ => None
    }
    idx.asInstanceOf[Option[GeoMesaFeatureIndex[T, U]]]
  }

  private def fromId(hint: String): Option[Seq[IndexId]] = Try(Seq(IndexId.id(hint))).toOption

  private def fromName(sft: SimpleFeatureType, hint: String): Option[Seq[IndexId]] = {
    available.find(i => hint.equalsIgnoreCase(i.name)).flatMap { i =>
      val defaults = i.defaults(sft).collect {
        case attributes if i.supports(sft, attributes) => IndexId(i.name, i.version, attributes)
      }
      if (defaults.isEmpty) { None } else { Some(defaults) }
    }
  }

  private def fromNameAndAttributes(sft: SimpleFeatureType, hint: String): Option[Seq[IndexId]] = {
    val Array(name, attributes @ _*) = hint.split(":")
    available.find(i => name.equalsIgnoreCase(i.name)).flatMap { i =>
      if (i.supports(sft, attributes)) { Some(Seq(IndexId(i.name, i.version, attributes))) } else { None }
    }
  }
}
