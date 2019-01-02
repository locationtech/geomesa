/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.io.ByteArrayOutputStream

import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, QueryPlan}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.ArrowScan._
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat, TopK}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag

trait ArrowScan extends AggregatingScan[ArrowAggregate] {

  private var batchSize: Int = _

  override def initResult(sft: SimpleFeatureType,
                          transform: Option[SimpleFeatureType],
                          options: Map[String, String]): ArrowAggregate = {
    import AggregatingScan.Configuration.{SftOpt, TransformSchemaOpt}
    import ArrowScan.Configuration._
    import ArrowScan.aggregateCache

    batchSize = options(BatchSizeKey).toInt

    val typ = options(TypeKey)
    val (arrowSft, arrowSftString) = transform match {
      case Some(tsft) => (tsft, options(TransformSchemaOpt))
      case None       => (sft, options(SftOpt))
    }
    val includeFids = options(IncludeFidsKey).toBoolean
    val proxyFids = options.get(ProxyFidsKey).exists(_.toBoolean)
    val dictionary = options(DictionaryKey)
    val sort = options.get(SortKey).map(name => (name, options.get(SortReverseKey).exists(_.toBoolean)))

    val cacheKey = typ + arrowSftString + includeFids + dictionary + sort

    def create(): ArrowAggregate = {
      val encoding = SimpleFeatureEncoding.min(includeFids, proxyFids)
      if (typ == Types.DeltaType) {
        val dictionaries = dictionary.split(",").filter(_.length > 0)
        new DeltaAggregate(arrowSft, dictionaries, encoding, sort, batchSize)
      } else if (typ == Types.BatchType) {
        val dictionaries = ArrowScan.decodeDictionaries(arrowSft, dictionary)
        sort match {
          case None => new BatchAggregate(arrowSft, dictionaries, encoding)
          case Some((s, r)) => new SortingBatchAggregate(arrowSft, dictionaries, encoding, s, r)
        }
      } else if (typ == Types.FileType) {
        val dictionaries = dictionary.split(",").filter(_.length > 0)
        // TODO file metadata created in the iterator has an empty sft name
        sort match {
          case None => new MultiFileAggregate(arrowSft, dictionaries, encoding)
          case Some((s, r)) => new MultiFileSortingAggregate(arrowSft, dictionaries, encoding, s, r)
        }
      } else {
        throw new RuntimeException(s"Expected type, got $typ")
      }
    }

    aggregateCache.getOrElseUpdate(cacheKey, create()).init(batchSize)
  }

  override protected def notFull(result: ArrowAggregate): Boolean = result.size < batchSize

  override protected def aggregateResult(sf: SimpleFeature, result: ArrowAggregate): Unit = result.add(sf)

  override protected def encodeResult(result: ArrowAggregate): Array[Byte] = result.encode()
}

object ArrowScan {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  object Configuration {

    val IncludeFidsKey = "fids"
    val ProxyFidsKey   = "proxy"
    val DictionaryKey  = "dict"
    val TypeKey        = "type"
    val BatchSizeKey   = "batch"
    val SortKey        = "sort"
    val SortReverseKey = "sort-rev"

    object Types {
      val BatchType = "batch"
      val DeltaType = "delta"
      val FileType  = "file"
    }
  }

  val DictionaryTopK = SystemProperty("geomesa.arrow.dictionary.top", "1000")

  val DictionaryOrdering: Ordering[AnyRef] = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowAggregate]

  /**
    * Configure the iterator
    *
    * @param sft simple feature type
    * @param index feature index
    * @param stats stats, used for querying dictionaries
    * @param filter full filter from the query, if any
    * @param ecql secondary push down filter, if any
    * @param hints query hints
    * @return
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                stats: GeoMesaStats,
                filter: Option[Filter],
                ecql: Option[Filter],
                hints: Hints): ArrowScanConfig = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    // handle sort from query
    setSortHints(hints)

    val arrowSft = hints.getTransformSchema.getOrElse(sft)
    val includeFids = hints.isArrowIncludeFid
    val proxyFids = hints.isArrowProxyFid
    val sort = hints.getArrowSort
    val batchSize = getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(includeFids, proxyFids)

    val baseConfig = {
      val base = AggregatingScan.configure(sft, index, ecql, hints.getTransform, hints.getSampling)
      base ++ AggregatingScan.optionalMap(
        IncludeFidsKey -> includeFids.toString,
        ProxyFidsKey   -> proxyFids.toString,
        SortKey        -> sort.map(_._1),
        SortReverseKey -> sort.map(_._2.toString),
        BatchSizeKey   -> batchSize.toString
      )
    }

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => k.property -> k).toMap
    }

    if (hints.isArrowDoublePass ||
          dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val dictionaries = createDictionaries(stats, sft, filter, dictionaryFields, providedDictionaries, cachedDictionaries)
      val config = baseConfig ++ Map(
        TypeKey       -> Configuration.Types.BatchType,
        DictionaryKey -> encodeDictionaries(dictionaries)
      )
      val reduce = mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort) _
      ArrowScanConfig(config, reduce)
    } else if (hints.isArrowMultiFile) {
      val config = baseConfig ++ Map(
        TypeKey       -> Configuration.Types.FileType,
        DictionaryKey -> dictionaryFields.mkString(",")
      )
      val reduce = mergeFiles(arrowSft, dictionaryFields, encoding, sort) _
      ArrowScanConfig(config, reduce)
    } else {
      val config = baseConfig ++ Map(
        TypeKey       -> Configuration.Types.DeltaType,
        DictionaryKey -> dictionaryFields.mkString(",")

      )
      val reduce = mergeDeltas(arrowSft, dictionaryFields, encoding, batchSize, sort) _
      ArrowScanConfig(config, reduce)
    }
  }

  /**
    * Converts standard sort hints from the query into arrow sort hints
    *
    * @param hints hints
    */
  def setSortHints(hints: Hints): Unit = {
    hints.getSortFields.foreach { sort =>
      if (sort.lengthCompare(1) > 0) {
        throw new IllegalArgumentException("Arrow queries only support sort by a single field: " +
            hints.getSortReadableString)
      } else if (sort.head._1.isEmpty) {
        throw new IllegalArgumentException("Arrow queries only support sort by properties: " +
            hints.getSortReadableString)
      } else {
        hints.getArrowSort match {
          case None =>
            hints.put(QueryHints.ARROW_SORT_FIELD, sort.head._1)
            hints.put(QueryHints.ARROW_SORT_REVERSE, sort.head._2)
          case Some(s) =>
            if (s != sort.head) {
              throw new IllegalArgumentException(s"Query sort does not match Arrow hints sort: " +
                  s"${hints.getSortReadableString} != ${s._1}:${if (s._2) "DESC" else "ASC"}")
            }
        }
        hints.remove(QueryHints.Internal.SORT_FIELDS)
      }
    }
  }

  /**
    * Gets the batch size from query hints, or falls back to the system property
    *
    * @param hints query hints
    * @return
    */
  def getBatchSize(hints: Hints): Int = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)

  /**
    * Reduce function for whole arrow files coming back from the aggregating scan. Each feature
    * will have a single arrow file
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding simple feature encoding
    * @param sort sort
    * @return
    */
  def mergeFiles(sft: SimpleFeatureType,
                 dictionaryFields: Seq[String],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)])
                (iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
    val result = SimpleFeatureArrowIO.reduceFiles(sft, dictionaryFields, encoding, sort)(bytes)
    val sf = resultFeature()
    result.map { bytes => sf.setAttribute(0, bytes); sf }
  }

  /**
    * Reduce function for batches with a common schema.
    *
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    * @param batchSize batch size
    * @param sort sort
    * @return
    */
  def mergeBatches(sft: SimpleFeatureType,
                   dictionaries: Map[String, ArrowDictionary],
                   encoding: SimpleFeatureEncoding,
                   batchSize: Int,
                   sort: Option[(String, Boolean)])
                  (iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val batches = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
    val result = SimpleFeatureArrowIO.reduceBatches(sft, dictionaries, encoding, sort, batchSize)(batches)
    val sf = resultFeature()
    result.map { bytes => sf.setAttribute(0, bytes); sf }
  }

  /**
    * Reduce function for delta batches.
    *
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries
    * @param encoding encoding
    * @param batchSize batch size
    * @param sort sort
    * @return
    */
  def mergeDeltas(sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  batchSize: Int,
                  sort: Option[(String, Boolean)])
                 (iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val files = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
    val result = DeltaWriter.reduce(sft, dictionaryFields, encoding, sort, batchSize)(files)
    val sf = resultFeature()
    result.map { bytes => sf.setAttribute(0, bytes); sf }
  }

  /**
    * Determine dictionary values, as required. Priority:
    *   1. values provided by the user
    *   2. cached topk stats
    *   3. enumeration stats query against result set
    *
    * @param stats stats
    * @param sft simple feature type
    * @param filter full filter for the query being run, used if querying enumeration values
    * @param attributes names of attributes to dictionary encode
    * @param provided provided dictionary values, if any, keyed by attribute name
    * @return
    */
  def createDictionaries(stats: GeoMesaStats,
                         sft: SimpleFeatureType,
                         filter: Option[Filter],
                         attributes: Seq[String],
                         provided: Map[String, Array[AnyRef]],
                         cached: Map[String, TopK[AnyRef]]): Map[String, ArrowDictionary] = {
    def sort(values: Array[AnyRef]): Unit = java.util.Arrays.sort(values, DictionaryOrdering)

    if (attributes.isEmpty) { Map.empty } else {
      var id = -1L
      // note: sort values to return same dictionary cache
      val providedDictionaries = provided.map { case (k, v) =>
        id += 1
        sort(v)
        k -> ArrowDictionary.create(id, v)(ClassTag[AnyRef](sft.getDescriptor(k).getType.getBinding))
      }
      val toLookup = attributes.filterNot(provided.contains)
      if (toLookup.isEmpty) { providedDictionaries } else {
        // use topk if available, otherwise run a live stats query to get the dictionary values
        val queried = if (toLookup.forall(cached.contains)) {
          cached.map { case (name, k) =>
            id += 1
            val values = k.topK(DictionaryTopK.get.toInt).map(_._1).toArray
            sort(values)
            name -> ArrowDictionary.create(id, values)(ClassTag[AnyRef](sft.getDescriptor(name).getType.getBinding))
          }
        } else {
          // if we have to run a query, might as well generate all values
          val query = Stat.SeqStat(toLookup.map(Stat.Enumeration))
          val enumerations = stats.runStats[EnumerationStat[String]](sft, query, filter.getOrElse(Filter.INCLUDE))
          // enumerations should come back in the same order
          // this has been fixed, but previously we couldn't use the enumeration attribute
          // number b/c it might reflect a transform sft
          val nameIter = toLookup.iterator
          enumerations.map { e =>
            id += 1
            val name = nameIter.next
            val values = e.values.toArray[AnyRef]
            sort(values)
            name -> ArrowDictionary.create(id, values)(ClassTag[AnyRef](sft.getDescriptor(name).getType.getBinding))
          }.toMap
        }
        queried ++ providedDictionaries
      }
    }
  }

  /**
    * Simple feature used for returning from scans
    *
    * @return
    */
  def resultFeature(): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(null, GeometryUtils.zeroPoint))

  /**
    * Encodes the dictionaries as a string for passing to the iterator config
    *
    * @param dictionaries dictionaries
    * @return
    */
  private def encodeDictionaries(dictionaries: Map[String, ArrowDictionary]): String =
    StringSerialization.encodeSeqMap(dictionaries.map { case (k, v) => k -> v.iterator.toSeq })

  /**
    * Decodes an encoded dictionary string from an iterator config
    *
    * @param encoded dictionary string
    * @return
    */
  private def decodeDictionaries(sft: SimpleFeatureType, encoded: String): Map[String, ArrowDictionary] = {
    var id = -1L
    StringSerialization.decodeSeqMap(sft, encoded).map { case (k, v) =>
      id += 1
      k -> ArrowDictionary.create(id, v)(ClassTag[AnyRef](sft.getDescriptor(k).getType.getBinding))
    }
  }

  /**
    * Trait for aggregating arrow files
    */
  trait ArrowAggregate {
    def size: Int
    def add(sf: SimpleFeature): Unit
    def encode(): Array[Byte]
    def clear(): Unit
    def init(size: Int): ArrowAggregate

    def isEmpty: Boolean = size == 0
  }

  /**
    * Returns full arrow files, with metadata. Builds dictionaries on the fly. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries fields
    * @param encoding encoding
    */
  class MultiFileAggregate(sft: SimpleFeatureType, dictionaryFields: Seq[String], encoding: SimpleFeatureEncoding)
      extends ArrowAggregate {

    private val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)
    private val os = new ByteArrayOutputStream()

    override def add(sf: SimpleFeature): Unit = writer.add(sf)

    override def size: Int = writer.size

    override def clear(): Unit = writer.clear()

    override def encode(): Array[Byte] = {
      os.reset()
      writer.encode(os)
      os.toByteArray
    }

    override def init(size: Int): ArrowAggregate = this
  }

  /**
    * Returns full arrow files, with metadata. Builds dictionaries on the fly. Sorts each file, but not between files
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries fields
    * @param encoding encoding
    * @param sortBy field to sort by
    * @param reverse sort reverse or not
    */
  class MultiFileSortingAggregate(sft: SimpleFeatureType,
                                  dictionaryFields: Seq[String],
                                  encoding: SimpleFeatureEncoding,
                                  sortBy: String,
                                  reverse: Boolean) extends ArrowAggregate {

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)
    private val os = new ByteArrayOutputStream()

    private val ordering = {
      val o = SimpleFeatureOrdering(sft.indexOf(sortBy))
      if (reverse) { o.reverse } else { o }
    }

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      index = 0
      writer.clear()
    }

    override def encode(): Array[Byte] = {
      java.util.Arrays.sort(features, 0, index, ordering)

      var i = 0
      while (i < index) {
        writer.add(features(i))
        i += 1
      }

      os.reset()
      writer.encode(os)
      os.toByteArray
    }

    override def init(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
    }
  }

  /**
    * Returns record batches without any metadata. Dictionaries must be known up front. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    */
  class BatchAggregate(sft: SimpleFeatureType,
                       dictionaries: Map[String, ArrowDictionary],
                       encoding: SimpleFeatureEncoding) extends ArrowAggregate {

    private var index = 0

    private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val batchWriter = new RecordBatchUnloader(vector)

    override def add(sf: SimpleFeature): Unit = {
      vector.writer.set(index, sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      vector.clear()
      index = 0
    }

    override def encode(): Array[Byte] = batchWriter.unload(index)

    override def init(size: Int): ArrowAggregate = this
  }

  /**
    * Returns record batches without any metadata. Dictionaries must be known up front. Sorts each batch,
    * but not between batches
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    * @param sortField sort field
    * @param reverse sort reverse
    */
  class SortingBatchAggregate(sft: SimpleFeatureType,
                              dictionaries: Map[String, ArrowDictionary],
                              encoding: SimpleFeatureEncoding,
                              sortField: String,
                              reverse: Boolean) extends ArrowAggregate {

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val batchWriter = new RecordBatchUnloader(vector)

    private val ordering = {
      val o = SimpleFeatureOrdering(sft.indexOf(sortField))
      if (reverse) { o.reverse } else { o }
    }

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      index = 0
      vector.clear()
    }

    override def encode(): Array[Byte] = {
      java.util.Arrays.sort(features, 0, index, ordering)

      var i = 0
      while (i < index) {
        vector.writer.set(i, features(i))
        i += 1
      }
      batchWriter.unload(index)
    }

    override def init(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
    }
  }

  /**
    * Returns batches of [threading key][dictionary deltas][record batch]. Will sort each batch,
    * but not between batches.
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding arrow encoding
    * @param sort sort field, sort reverse
    * @param initialSize initial batch size, will grow if needed
    */
  class DeltaAggregate(sft: SimpleFeatureType,
                       dictionaryFields: Seq[String],
                       encoding: SimpleFeatureEncoding,
                       sort: Option[(String, Boolean)],
                       initialSize: Int) extends ArrowAggregate {

    private val writer = new DeltaWriter(sft, dictionaryFields, encoding, sort, initialSize)
    private var index = 0
    private var features: Array[SimpleFeature] = _

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = index = 0

    override def encode(): Array[Byte] = writer.encode(features, index)

    override def init(size: Int): ArrowAggregate = {
      writer.reset()
      if (features == null || features.length < size) {
        features = Array.ofDim(size)
      }
      this
    }
  }

  case class ArrowScanConfig(config: Map[String, String], reduce: QueryPlan.Reducer)
}
