/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.NullableVarCharVector
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ArrowSimpleFeatureWriter(os: OutputStream, sft: SimpleFeatureType, dictionaryValues: Map[String, Seq[String]] = Map.empty)
    extends Closeable with Flushable {

  import scala.collection.JavaConversions._

  dictionaryValues.keys.foreach { attribute =>
    if (sft.getDescriptor(attribute).getType.getBinding != classOf[String]) {
      throw new NotImplementedError("Dictionaries only supported for string types")
    }
  }

  private val allocator = new RootAllocator(Long.MaxValue)
  private val dictionaries = dictionaryValues.mapValues(ArrowSimpleFeatureWriter.createDictionary(_, allocator))

  private val root = {
    val vector = new NullableMapVector("features", allocator, null, null)
    sft.getAttributeDescriptors.foreach { d =>
      val dictionary = dictionaries.get(d.getLocalName).map(_.getEncoding).orNull

      vector.addOrGet(d.getLocalName, dictionary)
    }

    vector.allocateNew()

    val writer = new ComplexWriterImpl("attributes", vector)
    val rootWriter = writer.rootAsMap()

    val idWriter = ArrowAttributeWriter("id", Seq(ObjectType.STRING), rootWriter, allocator)
    val attributeWriters = sft.getAttributeDescriptors.map(ad => ArrowAttributeWriter(ad, rootWriter, allocator)).toArray


  }
  private val provider = new MapDictionaryProvider()
  private val writer = new ArrowStreamWriter(root, provider, os)

  def start(): Unit = {

  }

  def add(feature: SimpleFeature): Unit = {

    var i = 0
    features.foreach { feature =>
      idWriter(i, feature.getID)
      var j = 0
      while (j < attributeWriters.length) {
        val attribute = feature.getAttribute(j)
        if (attribute != null) {
          attributeWriters(j).apply(i, attribute)
        }
        j += 1
      }
      i += 1
    }
    writer.setValueCount(i)
  }

  override def flush(): Unit = {

  }

  override def close(): Unit = {

  }
}

object ArrowSimpleFeatureWriter {

  private final val ids = new AtomicLong(0)

  private def createDictionary(values: Seq[String], allocator: BufferAllocator): Dictionary = {
    val id = ids.getAndIncrement()
    val vector = new NullableVarCharVector(s"dictionary-$id", allocator, null)
    val mutator = vector.getMutator
    var i = 0
    values.foreach { value =>
      if (value != null) {
        mutator.set(i, value.getBytes(StandardCharsets.UTF_8))
      }
      i += 1
    }
    mutator.setValueCount(i)

    val bitWidth = if (i > Short.MaxValue) { 32 } else if (i > Byte.MaxValue) { 16 } else { 8 }
    val encoding = new DictionaryEncoding(id, false, new ArrowType.Int(bitWidth, true))

    new Dictionary(vector, encoding)
  }
}