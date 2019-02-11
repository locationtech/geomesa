/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex.IdFromRow
import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex.IdFromAttributeRow
import org.locationtech.geomesa.index.index.z2.{XZ2IndexKeySpace, Z2IndexKeySpace}
import org.locationtech.geomesa.index.index.z3.{XZ3IndexKeySpace, Z3IndexKeySpace}
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
class AttributeIndex protected (ds: GeoMesaDataStore[_],
                                sft: SimpleFeatureType,
                                version: Int,
                                val attribute: String,
                                secondaries: Seq[String],
                                mode: IndexMode)
    extends GeoMesaFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey](ds, sft, AttributeIndex.name, version, secondaries.+:(attribute), mode)
        with AttributeFilterStrategy[AttributeIndexValues[Any], AttributeIndexKey] {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, AttributeIndex.version, attribute, secondaries, mode)

  private val attributeIndex = sft.indexOf(attribute)
  private val descriptor = sft.getDescriptor(attributeIndex)

  private val decodeValue: String => AnyRef = if (descriptor.isList) {
    // get the alias from the type of values in the collection
    val alias = descriptor.getListType().getSimpleName.toLowerCase(Locale.US)
    // Note that for collection types, only a single entry of the collection will be decoded - this is
    // because the collection entries have been broken up into multiple rows
    encoded => Collections.singletonList(AttributeIndexKey.decode(alias, encoded))
  } else {
    val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
    AttributeIndexKey.decode(alias, _)
  }

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

  // note: needs to be lazy to allow for keyspace override in subclasses
  protected lazy val rowValueOffset: Int = keySpace.sharding.length

  // note: needs to be lazy to allow for tiered keyspace override in subclasses
  override protected lazy val idFromRow: IdFromRow =
    new IdFromAttributeRow(sft, rowValueOffset, tieredKeySpace.map(_.indexKeyByteLength).getOrElse(0))

  /**
    * Decodes an attribute value out of row string
    *
    * @param row row bytes
    * @param offset offset into the row bytes
    * @param length length of the row bytes, from the offset
    * @return
    */
  def decodeRowValue(row: Array[Byte], offset: Int, length: Int): Try[AnyRef] = Try {
    val valueStart = offset + rowValueOffset // start of the encoded value
    // null byte indicates end of value
    val valueEnd = math.min(row.indexOf(ByteArrays.ZeroByte, valueStart), offset + length)
    decodeValue(new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8))
  }
}

object AttributeIndex extends ConfiguredIndex {

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

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = {
    sft.getAttributeDescriptors.asScala.flatMap { d =>
      val index = d.getUserData.get(AttributeOptions.OPT_INDEX).asInstanceOf[String]
      if (index != null &&
          (index.equalsIgnoreCase(IndexCoverage.FULL.toString) || java.lang.Boolean.valueOf(index)) &&
          AttributeIndexKey.encodable(d)) {
        Seq(Seq(d.getLocalName) ++ Option(sft.getGeomField) ++ sft.getDtgField.filter(_ != d.getLocalName))
      } else {
        Seq.empty
      }
    }
  }

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

  /**
    * Id from attribute index row
    *
    * @param sft simple feature type
    * @param rowValueOffset offset into the row for the value (usually the sharding prefix
    *                       and/or the attribute number for older index impls)
    * @param tierKeyLength tiered keyspace length
    */
  class IdFromAttributeRow(sft: SimpleFeatureType, rowValueOffset: Int, tierKeyLength: Int)
      extends IdFromRow(sft, 0) {

    private val idFromBytes = GeoMesaFeatureIndex.idFromBytes(sft)

    override def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String = {
      // null byte indicates end of value
      val start = row.indexOf(ByteArrays.ZeroByte, rowValueOffset + offset) + tierKeyLength + 1
      idFromBytes(row, start, length + offset - start, feature)
    }
  }
}
