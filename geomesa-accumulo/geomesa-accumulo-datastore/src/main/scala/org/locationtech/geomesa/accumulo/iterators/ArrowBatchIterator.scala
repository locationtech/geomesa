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
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.iterators.KryoLazyAggregatingIterator.SFT_OPT
import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator.{TRANSFORM_DEFINITIONS_OPT, TRANSFORM_SCHEMA_OPT}
import org.locationtech.geomesa.arrow.ArrowEncodedSft
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat, TopK}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Aggregates and returns arrow 'record batches'. An arrow file consists of metadata, n-batches, and a footer.
  * The metadata and footer are added by the reduce features step.
  */
class ArrowBatchIterator extends KryoLazyAggregatingIterator[ArrowBatchAggregate] with SamplingIterator {

  import ArrowBatchIterator._

  var aggregate: (SimpleFeature, ArrowBatchAggregate) => Unit = _
  var underBatchSize: (ArrowBatchAggregate) => Boolean = _

  override def init(options: Map[String, String]): ArrowBatchAggregate = {
    underBatchSize = options.get(BatchSizeKey).map(_.toInt) match {
      case None    => (_) => true
      case Some(i) => (a) => a.size < i
    }
    val encodedDictionaries = options(DictionaryKey)
    lazy val dictionaries = decodeDictionaries(encodedDictionaries)
    val includeFids = options(IncludeFidsKey).toBoolean
    val (arrowSft, arrowSftString) =
      if (hasTransform) { (transformSft, options(TRANSFORM_SCHEMA_OPT)) } else { (sft, options(SFT_OPT)) }
    aggregate = sample(options) match {
      case None       => (sf, result) => result.add(sf)
      case Some(samp) => (sf, result) => if (samp(sf)) { result.add(sf) }
    }
    aggregateCache.getOrElseUpdate(arrowSftString + includeFids + encodedDictionaries,
      new ArrowBatchAggregate(arrowSft, dictionaries, includeFids))
  }

  override def notFull(result: ArrowBatchAggregate): Boolean = underBatchSize(result)

  override def aggregateResult(sf: SimpleFeature, result: ArrowBatchAggregate): Unit = aggregate(sf, result)

  override def encodeResult(result: ArrowBatchAggregate): Array[Byte] = result.encode()
}

class ArrowBatchAggregate(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary], includeFids: Boolean) {

  import org.locationtech.geomesa.arrow.allocator

  import scala.collection.JavaConversions._

  private var index = 0

  private val vector = SimpleFeatureVector.create(sft, dictionaries, includeFids, GeometryPrecision.Float)
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

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val DefaultPriority = 25

  private val BatchSizeKey   = "batch"
  private val DictionaryKey  = "dict"
  private val IncludeFidsKey = "fids"

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowBatchAggregate]

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                dictionaries: Map[String, ArrowDictionary],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "arrow-batch-iter", classOf[ArrowBatchIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    hints.getSampling.foreach(SamplingIterator.configure(is, sft, _))
    hints.getArrowBatchSize.foreach(i => is.addOption(BatchSizeKey, i.toString))
    is.addOption(DictionaryKey, encodeDictionaries(dictionaries))
    is.addOption(IncludeFidsKey, hints.isArrowIncludeFid.toString)
    hints.getTransform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is
  }

  /**
    * Determine dictionary values, as required. Priority:
    *   1. values provided by the user
    *   2. cached topk stats
    *   3. enumeration stats query against result set
    *
    * @param ds data store
    * @param sft simple feature type
    * @param filter full filter for the query being run, used if querying enumeration values
    * @param attributes names of attributes to dictionary encode
    * @param provided provided dictionary values, if any, keyed by attribute name
    * @return
    */
  def createDictionaries(ds: AccumuloDataStore,
                         sft: SimpleFeatureType,
                         filter: Option[Filter],
                         attributes: Seq[String],
                         provided: Map[String, Seq[AnyRef]]): Map[String, ArrowDictionary] = {
    def name(i: Int): String = sft.getDescriptor(i).getLocalName

    if (attributes.isEmpty) { Map.empty } else {
      // note: sort values to return same dictionary cache
      val providedDictionaries = provided.map { case (k, v) => k -> ArrowDictionary.create(sort(v)) }
      val toLookup = attributes.filterNot(provided.contains)
      val lookedUpDictionaries = if (toLookup.isEmpty) { Map.empty } else {
        // use topk if available, otherwise run a live stats query to get the dictionary values
        val read = ds.stats.getStats[TopK[AnyRef]](sft, toLookup).map { k =>
          name(k.attribute) -> ArrowDictionary.create(sort(k.topK(1000).map(_._1)))
        }.toMap
        val toQuery = toLookup.filterNot(read.contains)
        val queried = if (toQuery.isEmpty) { Map.empty } else {
          val stats = Stat.SeqStat(toQuery.map(Stat.Enumeration))
          val enumerations = ds.stats.runStats[EnumerationStat[String]](sft, stats, filter.getOrElse(Filter.INCLUDE))
          enumerations.map { e => name(e.attribute) -> ArrowDictionary.create(sort(e.values.toSeq)) }.toMap
        }
        queried ++ read
      }
      providedDictionaries ++ lookedUpDictionaries
    }
  }

  private def sort(values: Seq[AnyRef]): Seq[AnyRef] = {
    if (values.isEmpty || !classOf[Comparable[_]].isAssignableFrom(values.head.getClass)) {
      values
    } else {
      implicit val ordering = new Ordering[AnyRef] {
        def compare(left: AnyRef, right: AnyRef): Int = left.asInstanceOf[Comparable[AnyRef]].compareTo(right)
      }
      values.sorted
    }
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", ArrowEncodedSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(0, e.getValue.get())
      sf
    }
  }

  /**
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature types
    * @param hints query hints
    * @param dictionaries dictionaries
    * @return
    */
  def reduceFeatures(sft: SimpleFeatureType,
                     hints: Hints,
                     dictionaries: Map[String, ArrowDictionary]):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
    val header = new ScalaSimpleFeature("", ArrowEncodedSft)
    header.setAttribute(0, fileMetadata(sft, dictionaries, hints.isArrowIncludeFid))
    header.setAttribute(1, GeometryUtils.zeroPoint)
    val footer = new ScalaSimpleFeature("", ArrowEncodedSft)
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
    * @param includeFids include feature ids or not
    * @return
    */
  private def fileMetadata(sft: SimpleFeatureType,
                           dictionaries: Map[String, ArrowDictionary],
                           includeFids: Boolean): Array[Byte] = {
    import org.locationtech.geomesa.arrow.allocator
    val out = new ByteArrayOutputStream
    val precision = GeometryPrecision.Float
    WithClose(new SimpleFeatureArrowFileWriter(sft, out, dictionaries, includeFids, precision)) { writer =>
      writer.start()
      out.toByteArray // copy bytes before closing so we get just the header metadata
    }
  }

  /**
    * Encodes the dictionaries as a string for passing to the iterator config
    *
    * @param dictionaries dictionaries
    * @return
    */
  private def encodeDictionaries(dictionaries: Map[String, ArrowDictionary]): String =
    StringSerialization.encodeSeqMap(dictionaries.map { case (k, v) => k -> v.values })

  /**
    * Decodes an encoded dictionary string from an iterator config
    *
    * @param encoded dictionary string
    * @return
    */
  private def decodeDictionaries(encoded: String): Map[String, ArrowDictionary] =
    StringSerialization.decodeSeqMap(encoded).map { case (k, v) => k -> ArrowDictionary.create(v) }
}
