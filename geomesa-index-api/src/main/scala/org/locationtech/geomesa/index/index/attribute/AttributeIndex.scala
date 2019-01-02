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

import com.google.common.primitives.Bytes
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.index._
import org.locationtech.geomesa.index.index.attribute.AttributeIndex.AttributeRowDecoder
import org.locationtech.geomesa.index.index.legacy.AttributeShardedIndex.typeRegistry
import org.locationtech.geomesa.index.index.z2.{XZ2IndexKeySpace, Z2IndexKeySpace}
import org.locationtech.geomesa.index.index.z3.{XZ3IndexKeySpace, Z3IndexKeySpace}
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try

trait AttributeIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends BaseTieredFeatureIndex[DS, F, W, R, C, AttributeIndexValues[Any], AttributeIndexKey]
        with AttributeFilterStrategy[DS, F, W] with AttributeRowDecoder {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = AttributeIndex.Name

  override protected val keySpace: AttributeIndexKeySpace = AttributeIndexKeySpace

  override protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    AttributeIndex.TieredOptions.find(_.supports(sft))

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = AttributeShardStrategy(sft)

  override def decodeRowValue(sft: SimpleFeatureType, index: Int): (Array[Byte], Int, Int) => Try[AnyRef] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val shards = if (shardStrategy(sft).shards.isEmpty) { 0 } else { 1 }
    // exclude feature byte and 2 index bytes and shard bytes
    val from = if (sft.isTableSharing) { 3 + shards } else { 2 + shards }
    val descriptor = sft.getDescriptor(index)
    val decode: (String) => AnyRef = if (descriptor.isList) {
      // get the alias from the type of values in the collection
      val alias = descriptor.getListType().getSimpleName.toLowerCase(Locale.US)
      // Note that for collection types, only a single entry of the collection will be decoded - this is
      // because the collection entries have been broken up into multiple rows
      (encoded) => Collections.singletonList(typeRegistry.decode(alias, encoded))
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, _)
    }
    (row, offset, length) => Try {
      val valueStart = offset + from // start of the encoded value
      // null byte indicates end of value
      val valueEnd = math.min(row.indexOf(ByteArrays.ZeroByte, valueStart), offset + length)
      decode(new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8))
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String = {
    val idFromBytes = GeoMesaFeatureIndex.idFromBytes(sft)
    // drop the encoded value and the secondary index field if it's present - the rest of the row is the ID
    val shards = if (shardStrategy(sft).shards.isEmpty) { 0 } else { 1 }
    // exclude feature byte and 2 index bytes and shard bytes
    val from = if (sft.isTableSharing) { 3 + shards } else { 2 + shards }
    val secondary = tieredKeySpace(sft).map(_.indexKeyByteLength).getOrElse(0)
    (row, offset, length, feature) => {
      val start = row.indexOf(ByteArrays.ZeroByte, from + offset) + secondary + 1
      idFromBytes(row, start, length + offset - start, feature)
    }
  }

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val sharing = sft.getTableSharingBytes
    val indices = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => sft.indexOf(d.getLocalName))
    val shards = nonEmpty(shardStrategy(sft).shards)

    // evaluate the splits per indexed attribute, instead of for the whole feature
    val result = indices.flatMap { indexOf =>
      val singleAttributeType = {
        val builder = new SimpleFeatureTypeBuilder()
        builder.setName(sft.getName)
        builder.add(sft.getDescriptor(indexOf))
        builder.buildFeatureType()
      }
      singleAttributeType.getUserData.putAll(sft.getUserData) // copy the splitter options
      val attribute = AttributeIndexKey.indexToBytes(indexOf)
      val splits = nonEmpty(TableSplitter.getSplits(singleAttributeType, name, partition))

      for (shard <- shards; split <- splits) yield {
        Bytes.concat(sharing, shard, attribute, split)
      }
    }

    // if not sharing, or the first feature in the table, drop the first split, which will otherwise be empty
    if (sharing.isEmpty || sharing.head == 0.toByte) {
      result.drop(1)
    } else {
      result
    }
  }
}

object AttributeIndex {

  val Name = "attr"

  val TieredOptions = Seq(Z3IndexKeySpace, XZ3IndexKeySpace, Z2IndexKeySpace, XZ2IndexKeySpace)

  trait AttributeRowDecoder {

    /**
      * Decodes an attribute value out of row string
      *
      * @param sft simple feature type
      * @param i attribute index
      * @return (bytes, offset, length) => decoded value
      */
    def decodeRowValue(sft: SimpleFeatureType, i: Int): (Array[Byte], Int, Int) => Try[Any]
  }
}
