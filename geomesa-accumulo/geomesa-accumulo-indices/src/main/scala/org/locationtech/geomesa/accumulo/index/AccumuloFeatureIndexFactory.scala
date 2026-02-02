/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.accumulo.index.legacy._
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.{ConfiguredIndex, DefaultFeatureIndexFactory}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.index.IndexCoverage

/**
  * Feature index factory that provides attribute join indices
  */
class AccumuloFeatureIndexFactory extends DefaultFeatureIndexFactory {

  import scala.collection.JavaConverters._

  override val all: Seq[ConfiguredIndex] = Seq(JoinIndex)

  override def fromFeatureFlag(sft: SimpleFeatureType, flag: String): Seq[IndexId] = {
    if (flag.equalsIgnoreCase(AttributeIndex.name)) {
      sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
        if (Option(d.getUserData.get(AttributeOptions.OptIndex)).exists(_.toString.equalsIgnoreCase(IndexCoverage.JOIN.toString))) {
          Some(joinIndex(sft, d))
        } else {
          None
        }
      }
    } else {
      // will handle "join"
      super.fromFeatureFlag(sft, flag)
    }
  }

  override def fromAttributeFlag(sft: SimpleFeatureType, descriptor: AttributeDescriptor, flag: String): Seq[IndexId] = {
    if (flag.equalsIgnoreCase(IndexCoverage.JOIN.toString)) {
      Seq(joinIndex(sft, descriptor))
    } else {
      // will handle "join:<attributes>"
      super.fromAttributeFlag(sft, descriptor, flag)
    }
  }

  private def joinIndex(sft: SimpleFeatureType, descriptor: AttributeDescriptor): IndexId =
    JoinIndex.indexFor(sft, descriptor).getOrElse {
      throw new IllegalArgumentException(
        s"Attribute '${descriptor.getLocalName}' is configured for join indexing but it is not a supported type: " +
          descriptor.getType.getBinding.getName)
    }

  override def create[T, U](ds: GeoMesaDataStore[_], sft: SimpleFeatureType, index: IndexId): Option[GeoMesaFeatureIndex[T, U]] = {
    val idx = if (index.name == JoinIndex.name) {
      val Seq(attribute, secondary @ _*) = index.attributes
      index.version match {
        case 8  => Some(new JoinIndex(ds, sft, attribute, secondary, index.mode))
        case 7  => Some(new JoinIndexV7(ds, sft, attribute, secondary, index.mode))
        case 6  => Some(new JoinIndexV6(ds, sft, attribute, secondary, index.mode))
        case 5  => Some(new JoinIndexV5(ds, sft, attribute, secondary, index.mode))
        case 4  => Some(new JoinIndexV4(ds, sft, attribute, secondary, index.mode))
        case 3  => Some(new JoinIndexV3(ds, sft, attribute, secondary.headOption, index.mode))
        case 2  => Some(new JoinIndexV2(ds, sft, attribute, secondary.headOption, index.mode))
        case _ => None
      }
    } else {
      None
    }
    idx.asInstanceOf[Option[GeoMesaFeatureIndex[T, U]]]
  }
}
