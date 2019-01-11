/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.index.attribute.AttributeIndex.AttributeRowDecoder
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Iterator that operates on kryo encoded attribute index rows. It will modify the value
  * (kryo encoded simple feature) by setting the attribute from the row key.
  *
  * It is expected to run *after* the normal iterator stack - thus the key value received in getTopValue
  * will be the already transformed feature result, with a null value for the attribute in question. This
  * iterator will set the attribute value based on the row, then re-serialize the feature
  */
class KryoAttributeKeyValueIterator extends SortedKeyValueIterator[Key, Value] with LazyLogging {

  import KryoAttributeKeyValueIterator._

  var source: SortedKeyValueIterator[Key, Value] = _
  val topValue: Value = new Value

  var sft: SimpleFeatureType = _
  var decodeRowValue: (Array[Byte], Int, Int) => Try[Any] = _
  var serializer: KryoFeatureSerializer = _
  var attribute: Int = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src

    val spec = options.get(SFT_OPT)
    sft = IteratorCache.sft(spec)
    attribute = options.get(ATTRIBUTE_OPT).toInt

    val index = try {
      AccumuloFeatureIndex.index(options.get(INDEX_OPT)).asInstanceOf[AccumuloFeatureIndex with AttributeRowDecoder]
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val kryoOptions = if (index.serializedWithId) SerializationOptions.none else SerializationOptions.withoutId
    serializer = IteratorCache.serializer(spec, kryoOptions)
    decodeRowValue = index.decodeRowValue(sft, attribute)
  }

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit =
    source.seek(range, columnFamilies, inclusive)

  override def next(): Unit = source.next()
  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey

  override def getTopValue: Value = {
    val serializedSf = source.getTopValue.get()
    val row = source.getTopKey.getRow
    val value = decodeRowValue(row.getBytes, 0, row.getLength).map { value =>
      val sf = serializer.deserialize(serializedSf)
      sf.setAttribute(attribute, value)
      serializer.serialize(sf)
    }
    topValue.set(value.getOrElse(serializedSf))
    topValue
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError
}

object KryoAttributeKeyValueIterator {

  val SFT_OPT       = "sft"
  val INDEX_OPT     = "index"
  val ATTRIBUTE_OPT = "attr"

  val DefaultPriority = 27 // needs to be higher than KryoLazyFilterTransformIterator at 25

  def configure(index: AccumuloFeatureIndexType,
                transform: SimpleFeatureType,
                attribute: String,
                priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "attr-value-iter", classOf[KryoAttributeKeyValueIterator])
    is.addOption(INDEX_OPT, index.identifier)
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(transform, includeUserData = true))
    is.addOption(ATTRIBUTE_OPT, transform.indexOf(attribute).toString)
    is
  }
}