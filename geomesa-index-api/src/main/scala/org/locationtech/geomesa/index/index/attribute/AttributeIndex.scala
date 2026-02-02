/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.{XZ2IndexKeySpace, Z2IndexKeySpace}
import org.locationtech.geomesa.index.index.z3.{XZ3IndexKeySpace, Z3IndexKeySpace}
import org.locationtech.geomesa.index.index.{ConfiguredIndex, NamedIndex}
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.IndexCoverage
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

import scala.util.Try

/**
  * Attribute index with configurable secondary index tiering. Each attribute has its own table
  *
  * @param ds data store
  * @param sft simple feature type stored in this index
  * @param version version of the index
  * @param attribute attribute field being indexed
  * @param secondaries secondary fields used for the index tiering
  * @param mode mode of the index (read/write/both)
  */
class AttributeIndex protected (
    ds: GeoMesaDataStore[_],
    sft: SimpleFeatureType,
    version: Int,
    attribute: String,
    secondaries: Seq[String],
    mode: IndexMode
  ) extends GeoMesaFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey](ds, sft, AttributeIndex.name, version, secondaries.+:(attribute), mode)
      with AttributeFilterStrategy[AttributeIndexValues[Any], AttributeIndexKey] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, AttributeIndex.version, attribute, secondaries, mode)

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpace(sft, AttributeShardStrategy(sft), attribute)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = {
    if (secondaries.isEmpty) { None } else {
      val opt = AttributeIndex.tiers.collectFirst {
        case ks if ks.supports(sft, secondaries) => ks.apply(sft, secondaries, tier = true)
      }
      if (opt.isEmpty) {
        throw new IllegalArgumentException(s"No key space matched tiering for attributes: ${secondaries.mkString(", ")}")
      }
      opt
    }
  }
}

object AttributeIndex extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  // note: keep z-indices before xz-indices, in order to use the most specific option available
  private val tiers = Seq(Z3IndexKeySpace, XZ3IndexKeySpace, Z2IndexKeySpace, XZ2IndexKeySpace, DateIndexKeySpace)

  val JoinIndexName = "join"

  override val name = "attr"
  override val version = 8

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean = {
    if (attributes.isEmpty) { false } else {
      val primary = attributes.take(1)
      val secondary = attributes.tail
      AttributeIndexKeySpace.supports(sft, primary) && (secondary.isEmpty || tiers.exists(_.supports(sft, secondary)))
    }
  }

  override def defaults(sft: SimpleFeatureType): Seq[IndexId] =
    defaults(sft, this, f => f.equalsIgnoreCase(IndexCoverage.FULL.toString) || java.lang.Boolean.valueOf(f))

  def defaults(sft: SimpleFeatureType, index: NamedIndex, flagCheck: String => Boolean): Seq[IndexId] = {
    sft.getAttributeDescriptors.asScala.toSeq.filter(AttributeIndexKey.encodable).flatMap { d =>
      d.getIndexFlags.flatMap { flag =>
        if (flag.equalsIgnoreCase(index.name) || flagCheck(flag)) {
          Seq(IndexId(index.name, index.version, defaultTiers(sft, d)))
        } else {
          // check for name:attr[:attr] and name:version[:attr]
          val Array(name, secondary@_*) = flag.split(":")
          if (name.equalsIgnoreCase(index.name)) {
            lazy val version = Try(secondary.head.toInt).toOption
            if (secondary.isEmpty) {
              Seq(IndexId(index.name, index.version, defaultTiers(sft, d)))
            } else if (supports(sft, Seq(d.getLocalName) ++ secondary)) {
              Seq(IndexId(index.name, index.version, Seq(d.getLocalName) ++ secondary))
            } else if (version.isDefined && supports(sft, Seq(d.getLocalName) ++ secondary.tail)) {
              Seq(IndexId(index.name, version.get, Seq(d.getLocalName) ++ secondary.tail))
            } else {
              throw new IllegalArgumentException(
                s"Attribute '${d.getLocalName}' is configured for indexing but index is not supported: $flag")
            }
          } else {
            Seq.empty
          }
        }
      }
    }
  }

  override def defaults(sft: SimpleFeatureType, primary: AttributeDescriptor): Option[IndexId] = {
    if (AttributeIndexKey.encodable(primary)) {
      Some(IndexId(name, version, defaultTiers(sft, primary)))
    } else {
      None
    }
  }

  private def defaultTiers(sft: SimpleFeatureType, primary: AttributeDescriptor): Seq[String] =
    Seq(primary.getLocalName) ++ Seq(sft.getGeomField).filter(_ != null) ++ sft.getDtgField.filter(_ != primary.getLocalName)

  /**
    * Checks if the given field is attribute indexed
    *
    * @param sft simple feature type
    * @param attribute attribute to check
    * @return
    */
  def indexed(sft: SimpleFeatureType, attribute: String): Boolean = {
    sft.getIndices.exists { i =>
      (i.name == name || i.name == JoinIndexName) && i.attributes.headOption.contains(attribute)
    }
  }
}
