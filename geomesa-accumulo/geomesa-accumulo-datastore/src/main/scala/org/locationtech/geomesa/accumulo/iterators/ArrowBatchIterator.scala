/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.WriteChannel
import org.apache.arrow.vector.stream.MessageSerializer
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.commons.lang3.StringEscapeUtils
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ArrowBatchIterator extends KryoLazyAggregatingIterator[ArrowBatchAggregate] {

  import ArrowBatchIterator.{DictionaryKey, decodeDictionaries}

  // TODO sampling
  override def init(options: Map[String, String]): ArrowBatchAggregate =
    new ArrowBatchAggregate(sft, decodeDictionaries(options(DictionaryKey)))

  override def aggregateResult(sf: SimpleFeature, result: ArrowBatchAggregate): Unit = result.add(sf)

  override def encodeResult(result: ArrowBatchAggregate): Array[Byte] = result.encode()
}

class ArrowBatchAggregate(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]) {

  import IteratorCache.allocator

  import scala.collection.JavaConversions._

  private var index = 0

  private val vector = SimpleFeatureVector.create(sft, dictionaries)
  private val root = new VectorSchemaRoot(Seq(vector.underlying.getField), Seq(vector.underlying), 0)
  private val unloader = new VectorUnloader(root)
  private val os = new ByteArrayOutputStream()

  def add(sf: SimpleFeature): Unit = {
    vector.writer.set(index, sf)
    index += 1
  }

  def isEmpty: Boolean = index == 0

  def clear(): Unit = {
    vector.reset()
    index = 0
  }

  def encode(): Array[Byte] = {
    os.reset()
    vector.writer.setValueCount(index)
    root.setRowCount(index)
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(os)), unloader.getRecordBatch)
    os.toByteArray
  }
}

object ArrowBatchIterator {

  val DefaultPriority = 25

  // need to be lazy to avoid class loading issues before init is called
  lazy val ArrowSft = SimpleFeatureTypes.createType("arrow", "batch:Bytes,*geom:Point:srid=4326")

  private val DictionaryKey = "dict"
  private val Tab = '\t'

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                dictionaries: Map[String, ArrowDictionary],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DefaultPriority): IteratorSetting = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val is = new IteratorSetting(priority, "arrow-iter", classOf[ArrowBatchIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    hints.getSampling.foreach(SamplingIterator.configure(is, sft, _))
    is.addOption(DictionaryKey, encodeDictionaries(dictionaries))
    is
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", ArrowSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(0, e.getValue.get())
      sf
    }
  }

  /**
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches
    *
    * @param sft simple feature types
    * @param dictionaries dictionaries
    * @return
    */
  def reduceFeatures(sft: SimpleFeatureType,
                     dictionaries: Map[String, ArrowDictionary]):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
    val header = new ScalaSimpleFeature("", ArrowSft)
    header.setAttribute(0, fileMetadata(sft, dictionaries))
    header.setAttribute(1, GeometryUtils.zeroPoint)
    val footer = new ScalaSimpleFeature("", ArrowSft)
    footer.setAttribute(0, Array[Byte](0, 0, 0, 0))
    footer.setAttribute(1, GeometryUtils.zeroPoint)
    (iter) => CloseableIterator(Iterator(header)) ++ iter ++ CloseableIterator(Iterator(footer))
  }

  private def fileMetadata(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Array[Byte] = {
    implicit val allocator = new RootAllocator(Long.MaxValue)
    try {
      val out = new ByteArrayOutputStream
      val writer = new SimpleFeatureArrowFileWriter(sft, out, dictionaries)
      writer.start()
      val bytes = out.toByteArray // copy bytes before closing so we just get the header metadata
      writer.close()
      bytes
    } finally {
      allocator.close()
    }
  }

  private def encodeDictionaries(dictionaries: Map[String, ArrowDictionary]): String = {
    val sb = new StringBuilder()
    dictionaries.foreach { case (name, dictionary) =>
      sb.append(name).append(Tab)
      dictionary.values.foreach { value =>
        sb.append(StringEscapeUtils.escapeJava(value.asInstanceOf[String])).append(Tab)
      }
      sb.append(Tab)
    }
    sb.toString
  }

  private def decodeDictionaries(encoded: String): Map[String, ArrowDictionary] = {
    encoded.split(s"$Tab$Tab").map { e =>
      val values = e.split(Tab)
      values.head -> new ArrowDictionary(values.tail)
    }.toMap
  }
}
