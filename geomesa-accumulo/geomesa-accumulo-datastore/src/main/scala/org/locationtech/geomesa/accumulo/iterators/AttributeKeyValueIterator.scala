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
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Iterator that operates on kryo encoded attribute index rows. It will modify the value
  * (kryo encoded simple feature) by setting the attribute from the row key.
  *
  * It is expected to run *after* the normal iterator stack - thus the key value received in getTopValue
  * will be the already transformed feature result, with a null value for the attribute in question. This
  * iterator will set the attribute value based on the row, then re-serialize the feature
  */
class AttributeKeyValueIterator extends SortedKeyValueIterator[Key, Value] with LazyLogging {

  import AttributeKeyValueIterator._

  var source: SortedKeyValueIterator[Key, Value] = _
  val topValue: Value = new Value

  var index: AttributeIndex = _
  var serializer: KryoFeatureSerializer = _
  var attribute: Int = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src

    val spec = options.get(SFT_OPT)
    attribute = options.get(ATTRIBUTE_OPT).toInt
    index = IteratorCache.index(IteratorCache.sft(spec), spec, options.get(INDEX_OPT)).asInstanceOf[AttributeIndex]
    val kryoOptions = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    serializer = IteratorCache.serializer(options.get(TRANSFORM_OPT), kryoOptions)
  }

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit =
    source.seek(range, columnFamilies, inclusive)

  override def next(): Unit = source.next()
  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey

  override def getTopValue: Value = {
    val serializedSf = source.getTopValue.get()
    val row = source.getTopKey.getRow
    val value = index.keySpace.decodeRowValue(row.getBytes, 0, row.getLength).map { value =>
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

object AttributeKeyValueIterator {

  val SFT_OPT       = "sft"
  val TRANSFORM_OPT = "tsft"
  val INDEX_OPT     = "index"
  val ATTRIBUTE_OPT = "attr"

  val DefaultPriority = 27 // needs to be higher than FilterTransformIterator at 25

  def configure(index: AttributeIndex,
                transform: SimpleFeatureType,
                priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "attr-value-iter", classOf[AttributeKeyValueIterator])
    is.addOption(INDEX_OPT, index.identifier)
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(index.sft, includeUserData = true))
    is.addOption(TRANSFORM_OPT, SimpleFeatureTypes.encodeType(transform, includeUserData = true))
    is.addOption(ATTRIBUTE_OPT, transform.indexOf(index.attribute).toString)
    is
  }
}