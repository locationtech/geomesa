/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaFeatureIndexFactory}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.attribute.legacy._
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.id.legacy.{IdIndexV1, IdIndexV2, IdIndexV3}
import org.locationtech.geomesa.index.index.s2.S2Index
import org.locationtech.geomesa.index.index.s3.S3Index
import org.locationtech.geomesa.index.index.z2.legacy._
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.legacy._
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.IndexCoverage

import scala.util.Try

/**
  * Feature index factory providing the default indices shipped with geomesa. Note: this class is not SPI loaded
  */
object DefaultFeatureIndexFactory extends DefaultFeatureIndexFactory {

  override protected val available: Seq[ConfiguredIndex] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, S3Index, S2Index, IdIndex, AttributeIndex)

  override def fromAttributeFlag(sft: SimpleFeatureType, descriptor: AttributeDescriptor, flag: String): Seq[IndexId] = {
    if (java.lang.Boolean.valueOf(flag) || flag.equalsIgnoreCase(IndexCoverage.FULL.toString)) {
      val indices = Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, AttributeIndex).flatMap(_.indexFor(sft, descriptor))
      if (indices.isEmpty) {
        throw new IllegalArgumentException(
          s"Attribute '${descriptor.getLocalName}' is configured for indexing but it is not a supported type: " +
            descriptor.getType.getBinding.getName)
      }
      indices
    } else {
      super.fromAttributeFlag(sft, descriptor, flag)
    }
  }

  override def create[T, U](ds: GeoMesaDataStore[_], sft: SimpleFeatureType, index: IndexId): Option[GeoMesaFeatureIndex[T, U]] = {

    import org.locationtech.geomesa.index.index.attribute.{AttributeIndex => AtIndex}

    lazy val Seq(geom3, dtg) = index.attributes
    lazy val Seq(geom2) = index.attributes
    lazy val Seq(attribute, secondary @ _*) = index.attributes

    val idx = (index.name, index.version) match {
      case (IdIndex.name, 4)  => Some(new IdIndex(ds, sft, index.mode))
      case (IdIndex.name, 3)  => Some(new IdIndexV3(ds, sft, index.mode))
      case (IdIndex.name, 2)  => Some(new IdIndexV2(ds, sft, index.mode))
      case (IdIndex.name, 1)  => Some(new IdIndexV1(ds, sft, index.mode))

      case (Z3Index.name, 7)  => Some(new Z3Index(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 6)  => Some(new Z3IndexV6(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 5)  => Some(new Z3IndexV5(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 4)  => Some(new Z3IndexV4(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 3)  => Some(new Z3IndexV3(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 2)  => Some(new Z3IndexV2(ds, sft, geom3, dtg, index.mode))
      case (Z3Index.name, 1)  => Some(new Z3IndexV1(ds, sft, geom3, dtg, index.mode))

      case (XZ3Index.name, 3) => Some(new XZ3Index(ds, sft, geom3, dtg, index.mode))
      case (XZ3Index.name, 2) => Some(new XZ3IndexV2(ds, sft, geom3, dtg, index.mode))
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
}

/**
 * Base feature index factory trait
 */
trait DefaultFeatureIndexFactory extends GeoMesaFeatureIndexFactory with LazyLogging {

  protected val available: Seq[ConfiguredIndex]

  override def all(): Seq[NamedIndex] = available

  override def indices(sft: SimpleFeatureType, hint: Option[String]): Seq[IndexId] =
    GeoMesaFeatureIndexFactory.indices(sft).filter(id => available.exists(_.name == id.name))

  override def fromFeatureFlag(sft: SimpleFeatureType, flag: String): Seq[IndexId] = {
    // check for name:attr[:attr] and name:version[:attr]
    val Array(name, secondary@_*) = flag.split(":")
    available.find(_.name.equalsIgnoreCase(name)).toSeq.flatMap { i =>
      lazy val version = Try(secondary.head.toInt).toOption
      if (secondary.isEmpty) {
        i.defaultIndicesFor(sft)
      } else if (i.supports(sft, secondary)) {
        Some(IndexId(i.name, i.version, secondary))
      } else if (version.isDefined && i.supports(sft, secondary.tail)) {
        Some(IndexId(i.name, version.get, secondary.tail))
      } else {
        None
      }
    }
  }

  override def fromAttributeFlag(sft: SimpleFeatureType, descriptor: AttributeDescriptor, flag: String): Seq[IndexId] = {
    if (flag.equalsIgnoreCase("false") || flag.equalsIgnoreCase("none")) {
      Seq(IndexId(EmptyIndex.name, EmptyIndex.version, Seq(descriptor.getLocalName)))
    } else {
      // check for name:attr[:attr] and name:version[:attr]
      val Array(name, secondary@_*) = flag.split(":")
      available.find(_.name.equalsIgnoreCase(name)).toSeq.flatMap { i =>
        lazy val version = Try(secondary.head.toInt).toOption
        if (secondary.isEmpty) {
          i.indexFor(sft, descriptor)
        } else if (i.supports(sft, Seq(descriptor.getLocalName) ++ secondary)) {
          Some(IndexId(i.name, i.version, Seq(descriptor.getLocalName) ++ secondary))
        } else if (version.isDefined && i.supports(sft, Seq(descriptor.getLocalName) ++ secondary.tail)) {
          Some(IndexId(i.name, version.get, Seq(descriptor.getLocalName) ++ secondary.tail))
        } else {
          None
        }
      }
    }
  }
}
