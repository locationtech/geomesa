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
import org.apache.arrow.vector.file.WriteChannel
import org.apache.arrow.vector.stream.MessageSerializer
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.commons.lang3.StringEscapeUtils
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.iterators.KryoLazyAggregatingIterator.SFT_OPT
import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator.{TRANSFORM_DEFINITIONS_OPT, TRANSFORM_SCHEMA_OPT}
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ArrowBatchIterator extends KryoLazyAggregatingIterator[ArrowBatchAggregate] with SamplingIterator {

  import ArrowBatchIterator.{BatchSizeKey, DictionaryKey, aggregateCache, decodeDictionaries}

  var aggregate: (SimpleFeature, ArrowBatchAggregate) => Unit = _
  var underBatchSize: (ArrowBatchAggregate) => Boolean = _

  override def init(options: Map[String, String]): ArrowBatchAggregate = {
    underBatchSize = options.get(BatchSizeKey).map(_.toInt) match {
      case None    => (_) => true
      case Some(i) => (a) => a.size < i
    }
    val encodedDictionaries = options(DictionaryKey)
    lazy val dictionaries = decodeDictionaries(encodedDictionaries)
    val transformSchema = options.get(TRANSFORM_SCHEMA_OPT).map(IteratorCache.sft).orNull
    if (transformSchema == null) {
      sample(options) match {
        case None       => aggregate = (sf, result) => result.add(sf)
        case Some(samp) => aggregate = (sf, result) => if (samp(sf)) { result.add(sf) }
      }
      aggregateCache.getOrElseUpdate(options(SFT_OPT) + encodedDictionaries, new ArrowBatchAggregate(sft, dictionaries))
    } else {
      val transforms = TransformSimpleFeature.attributes(sft, transformSchema, options(TRANSFORM_DEFINITIONS_OPT))
      val reusable = new TransformSimpleFeature(transformSchema, transforms)
      sample(options) match {
        case None       => aggregate = (sf, result) => { reusable.setFeature(sf); result.add(reusable) }
        case Some(samp) => aggregate = (sf, result) => if (samp(sf)) { reusable.setFeature(sf); result.add(reusable)  }
      }
      aggregateCache.getOrElseUpdate(options(TRANSFORM_DEFINITIONS_OPT) + encodedDictionaries,
        new ArrowBatchAggregate(transformSchema, dictionaries))
    }
  }

  override def notFull(result: ArrowBatchAggregate): Boolean = underBatchSize(result)

  override def aggregateResult(sf: SimpleFeature, result: ArrowBatchAggregate): Unit = aggregate(sf, result)

  override def encodeResult(result: ArrowBatchAggregate): Array[Byte] = result.encode()
}

class ArrowBatchAggregate(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]) {

  import org.locationtech.geomesa.arrow.allocator

  import scala.collection.JavaConversions._

  private var index = 0

  private val vector = SimpleFeatureVector.create(sft, dictionaries, GeometryPrecision.Float)
  private val root = new VectorSchemaRoot(Seq(vector.underlying.getField), Seq(vector.underlying), 0)
  private val unloader = new VectorUnloader(root)
  private val os = new ByteArrayOutputStream()

  def add(sf: SimpleFeature): Unit = {
    vector.writer.set(index, sf)
    index += 1
  }

  def isEmpty: Boolean = index == 0

  def size: Int = index

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

  private val BatchSizeKey  = "grp"
  private val DictionaryKey = "dict"
  private val Tab = '\t'

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowBatchAggregate]

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
    hints.getArrowBatchSize.foreach(i => is.addOption(BatchSizeKey, i.toString))
    is.addOption(DictionaryKey, encodeDictionaries(dictionaries))
    hints.getTransform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is
  }

  def createDictionaries(ds: AccumuloDataStore,
                         sft: SimpleFeatureType,
                         fields: Seq[String],
                         filter: Option[Filter]): Map[String, ArrowDictionary] = {
    if (fields.isEmpty) { Map.empty } else {
      require(fields.forall(sft.getDescriptor(_).getType.getBinding == classOf[String]),
        "Only string types supported for dictionary encoding")
      // run a live stats query to get the dictionary values
      val stats = Stat.SeqStat(fields.map(Stat.Enumeration))
      val enumerations = ds.stats.runStats[EnumerationStat[String]](sft, stats, filter.getOrElse(Filter.INCLUDE))
      // note: sort values to return same dictionary cache
      enumerations.map(e => sft.getDescriptor(e.attribute).getLocalName -> new ArrowDictionary(e.values.toSeq.sorted)()).toMap
    }
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
    // per arrow streaming format footer is the encoded int '0'
    footer.setAttribute(0, Array[Byte](0, 0, 0, 0))
    footer.setAttribute(1, GeometryUtils.zeroPoint)
    (iter) => CloseableIterator(Iterator(header)) ++ iter ++ CloseableIterator(Iterator(footer))
  }

  /**
    * Creates the header for the arrow file, which includes the schema and any dictionaries
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @return
    */
  private def fileMetadata(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Array[Byte] = {
    import org.locationtech.geomesa.arrow.allocator
    val out = new ByteArrayOutputStream
    val writer = new SimpleFeatureArrowFileWriter(sft, out, dictionaries, GeometryPrecision.Float)
    writer.start()
    val bytes = out.toByteArray // copy bytes before closing so we just get the header metadata
    writer.close()
    bytes
  }

  /**
    * Encodes the dictionaries as a string for passing to the iterator config
    *
    * @param dictionaries dictionaries
    * @return
    */
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

  /**
    * Decodes an encoded dictionary string from an iterator config
    *
    * @param encoded dictionary string
    * @return
    */
  private def decodeDictionaries(encoded: String): Map[String, ArrowDictionary] = {
    encoded.split(s"$Tab$Tab").map { e =>
      val values = e.split(Tab)
      values.head -> new ArrowDictionary(values.tail)()
    }.toMap
  }
}
