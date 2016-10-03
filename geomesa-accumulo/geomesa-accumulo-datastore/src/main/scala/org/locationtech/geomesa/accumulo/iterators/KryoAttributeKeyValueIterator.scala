/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex._
import org.locationtech.geomesa.accumulo.index.attribute.AttributeWritableIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
 * Iterator that operates on kryo encoded attribute index rows. It will modify the value
  * (kryo encoded simple feature) by setting the attribute from the row key.
 */
class KryoAttributeKeyValueIterator extends SortedKeyValueIterator[Key, Value] with LazyLogging {

  import KryoAttributeKeyValueIterator._

  var source: SortedKeyValueIterator[Key, Value] = null
  val topValue: Value = new Value

  var sft: SimpleFeatureType = null
  var serializer: KryoFeatureSerializer = null
  var attribute: Int = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {

    IteratorClassLoader.initClassLoader(getClass)

    this.source = src.deepCopy(env)
    sft = SimpleFeatureTypes.createType("", options.get(SFT_OPT))
    attribute = options.get(ATTRIBUTE_OPT).toInt

    val index = try { AccumuloFeatureIndex.index(options.get(INDEX_OPT)) } catch {
      case NonFatal(e) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val kryoOptions = if (index.serializedWithId) SerializationOptions.none else SerializationOptions.withoutId
    serializer = new KryoFeatureSerializer(sft, kryoOptions)
  }

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit =
    source.seek(range, columnFamilies, inclusive)

  override def next(): Unit = source.next()
  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey

  override def getTopValue: Value = {
    val serializedSf = source.getTopValue.get()
    val value = AttributeWritableIndex.decodeRow(sft, attribute, source.getTopKey.getRow).map { value =>
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

  def configure(index: AccumuloFeatureIndex,
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