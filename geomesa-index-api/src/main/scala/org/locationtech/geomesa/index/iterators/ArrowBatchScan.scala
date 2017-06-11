/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.io.ByteArrayOutputStream

import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat, TopK}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.math.Ordering

/**
  * Aggregates and returns arrow 'record batches'. An arrow file consists of metadata, n-batches, and a footer.
  * The metadata and footer are added by the reduce features step.
  */
trait ArrowBatchScan extends AggregatingScan[ArrowBatchAggregate] {

  private var batchSize: Int = _

  override def initResult(sft: SimpleFeatureType,
                          transform: Option[SimpleFeatureType],
                          options: Map[String, String]): ArrowBatchAggregate = {
    import AggregatingScan.Configuration.{SftOpt, TransformSchemaOpt}
    import ArrowBatchScan.Configuration._
    import ArrowBatchScan.{aggregateCache, decodeDictionaries}

    batchSize = options(BatchSizeKey).toInt
    val encodedDictionaries = options(DictionaryKey)
    lazy val dictionaries = decodeDictionaries(encodedDictionaries)
    val encoding = SimpleFeatureEncoding.min(options(IncludeFidsKey).toBoolean)
    val (arrowSft, arrowSftString) = transform match {
      case Some(tsft) => (tsft, options(TransformSchemaOpt))
      case None       => (sft, options(SftOpt))
    }
    val sortIndex = options.get(SortKey).map(arrowSft.indexOf).getOrElse(-1)
    val sortReverse = options.get(SortReverseKey).exists(_.toBoolean)
    aggregateCache.getOrElseUpdate(arrowSftString + encoding + sortIndex + sortReverse + encodedDictionaries,
      if (sortIndex == -1) { new ArrowBatchAggregateImpl(arrowSft, dictionaries, encoding) } else {
        new ArrowSortingBatchAggregate(arrowSft, sortIndex, sortReverse, batchSize, dictionaries, encoding) } )
  }

  override protected def notFull(result: ArrowBatchAggregate): Boolean = result.size < batchSize

  override protected def aggregateResult(sf: SimpleFeature, result: ArrowBatchAggregate): Unit = result.add(sf)

  override protected def encodeResult(result: ArrowBatchAggregate): Array[Byte] = result.encode()
}


trait ArrowBatchAggregate {
  def isEmpty: Boolean
  def clear(): Unit
  def add(sf: SimpleFeature): Unit
  def size: Int
  def encode(): Array[Byte]
}

class ArrowBatchAggregateImpl(sft: SimpleFeatureType,
                              dictionaries: Map[String, ArrowDictionary],
                              encoding: SimpleFeatureEncoding) extends ArrowBatchAggregate {

  import org.locationtech.geomesa.arrow.allocator

  private var index = 0

  private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
  private val batchWriter = new RecordBatchUnloader(vector)

  override def add(sf: SimpleFeature): Unit = {
    vector.writer.set(index, sf)
    index += 1
  }

  override def isEmpty: Boolean = index == 0

  override def size: Int = index

  override def clear(): Unit = {
    vector.clear()
    index = 0
  }

  override def encode(): Array[Byte] = batchWriter.unload(index)
}

class ArrowSortingBatchAggregate(sft: SimpleFeatureType,
                                 sortField: Int,
                                 reverse: Boolean,
                                 batchSize: Int,
                                 dictionaries: Map[String, ArrowDictionary],
                                 encoding: SimpleFeatureEncoding) extends ArrowBatchAggregate {

  import org.locationtech.geomesa.arrow.allocator

  private var index = 0
  private val features = Array.ofDim[SimpleFeature](batchSize)

  private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
  private val batchWriter = new RecordBatchUnloader(vector)

  private val ordering = new Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = {
      val left = x.getAttribute(sortField).asInstanceOf[Comparable[Any]]
      val right = y.getAttribute(sortField).asInstanceOf[Comparable[Any]]
      left.compareTo(right)
    }
  }

  override def add(sf: SimpleFeature): Unit = {
    // we have to copy since the feature might be re-used
    // TODO we could probably optimize this...
    features(index) = ScalaSimpleFeature.copy(sf)
    index += 1
  }

  override def isEmpty: Boolean = index == 0

  override def size: Int = index

  override def clear(): Unit = {
    index = 0
    vector.clear()
  }

  override def encode(): Array[Byte] = {
    java.util.Arrays.sort(features, 0, index, if (reverse) { ordering.reverse } else { ordering })

    var i = 0
    while (i < index) {
      vector.writer.set(i, features(i))
      i += 1
    }
    batchWriter.unload(index)
  }
}

object ArrowBatchScan {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  object Configuration {
    val BatchSizeKey   = "batch"
    val DictionaryKey  = "dict"
    val IncludeFidsKey = "fids"
    val SortKey        = "sort"
    val SortReverseKey = "sort-rev"
  }

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowBatchAggregate]

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                dictionaries: Map[String, ArrowDictionary],
                hints: Hints): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    val batchSize = hints.getArrowBatchSize.map(_.toString).getOrElse(ArrowProperties.BatchSize.get)

    val base = AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling)
    base ++ AggregatingScan.optionalMap(
      BatchSizeKey   -> batchSize,
      DictionaryKey  -> encodeDictionaries(dictionaries),
      IncludeFidsKey -> hints.isArrowIncludeFid.toString,
      SortKey        -> hints.getArrowSort.map(_._1),
      SortReverseKey -> hints.getArrowSort.map(_._2.toString)
    )
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
  def createDictionaries(ds: HasGeoMesaStats,
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
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)
    val sortField = hints.getArrowSort
    val header = new ScalaSimpleFeature("", ArrowEncodedSft,
      Array(fileMetadata(sft, dictionaries, encoding, sortField), GeometryUtils.zeroPoint))
    // per arrow streaming format footer is the encoded int '0'
    val footer = new ScalaSimpleFeature("", ArrowEncodedSft, Array(Array[Byte](0, 0, 0, 0), GeometryUtils.zeroPoint))
    val sort: CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = sortField match {
      case None => (iter) => iter
      case Some((attribute, reverse)) =>
        val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
        (iter) => {
          import SimpleFeatureArrowIO.sortBatches
          val sf = new ScalaSimpleFeature("", ArrowEncodedSft, Array(null, GeometryUtils.zeroPoint))
          val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
          val sorted = sortBatches(sft, dictionaries, encoding, attribute, reverse, batchSize, bytes)
          sorted.map { bytes => sf.setAttribute(0, bytes); sf }
        }

    }
    (iter) => CloseableIterator(Iterator(header)) ++ sort(iter) ++ CloseableIterator(Iterator(footer))
  }

  /**
    * Creates the header for the arrow file, which includes the schema and any dictionaries
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding options
    * @param sort data is sorted or not
    * @return
    */
  private def fileMetadata(sft: SimpleFeatureType,
                           dictionaries: Map[String, ArrowDictionary],
                           encoding: SimpleFeatureEncoding,
                           sort: Option[(String, Boolean)]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    WithClose(new SimpleFeatureArrowFileWriter(sft, out, dictionaries, encoding, sort)) { writer =>
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