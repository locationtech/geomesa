/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._

import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.tools.export.formats.ArrowExporter.{BatchDelegate, DictionaryDelegate, EncodedDelegate, SortedBatchDelegate}
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag

class ArrowExporter(
    os: OutputStream,
    counter: ByteCounter,
    hints: Hints,
    queryDictionaries: => Map[String, Array[AnyRef]]
  ) extends ByteCounterExporter(counter) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private lazy val sort = hints.getArrowSort
  private lazy val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)
  private lazy val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
  private lazy val dictionaryFields = hints.getArrowDictionaryFields

  private var delegate: FeatureExporter = _

  override def start(sft: SimpleFeatureType): Unit = {
    delegate = if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      new EncodedDelegate(os)
    } else {
      val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
      if (dictionaryFields.forall(providedDictionaries.contains)) {
        var id = -1
        val dictionaries = (queryDictionaries ++ providedDictionaries).map { case (k, v) =>
          id += 1
          k -> ArrowDictionary.create(id, v)(ClassTag[AnyRef](sft.getDescriptor(k).getType.getBinding))
        }
        sort match {
          case None    => new BatchDelegate(os, encoding, batchSize, dictionaries)
          case Some(s) => new SortedBatchDelegate(os, encoding, s, batchSize, dictionaries)
        }
      } else {
        if (sort.isDefined) {
          throw new NotImplementedError("Sorting and calculating dictionaries at the same time is not supported")
        }
        new DictionaryDelegate(os, dictionaryFields, encoding, batchSize)
      }
    }
    delegate.start(sft)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = delegate.export(features)

  override def close(): Unit = {
    Option(delegate).foreach(CloseWithLogging.apply)
    os.close()
  }
}

object ArrowExporter {

  import org.locationtech.geomesa.arrow.allocator

  def queryDictionaries(ds: DataStore, query: Query): Map[String, Array[AnyRef]] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConversions._

    val hints = query.getHints
    val dictionaryFields = {
      val provided = hints.getArrowDictionaryEncodedValues(ds.getSchema(query.getTypeName))
      hints.getArrowDictionaryFields.filterNot(provided.contains)
    }

    if (dictionaryFields.isEmpty) { Map.empty } else {
      // if we're hitting this, we can't do a stats query as we're not dealing with a geomesa store
      val dictionaryQuery = new Query(query.getTypeName, query.getFilter)
      dictionaryQuery.setPropertyNames(dictionaryFields)
      val map = dictionaryFields.map(f => f -> scala.collection.mutable.HashSet.empty[AnyRef]).toMap
      SelfClosingIterator(ds.getFeatureReader(dictionaryQuery, Transaction.AUTO_COMMIT)).foreach { sf =>
        map.foreach { case (k, values) => Option(sf.getAttribute(k)).foreach(values.add) }
      }
      map.map { case (k, values) => (k, values.toArray) }
    }
  }

  private class EncodedDelegate(os: OutputStream) extends FeatureExporter {
    override def start(sft: SimpleFeatureType): Unit = {}
    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      // just copy bytes directly out
      features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
      None // we don't know the actual count
    }
    override def bytes: Long = 0L
    override def close(): Unit = {}
  }

  private class DictionaryDelegate(
      os: OutputStream,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      batchSize: Int
    ) extends FeatureExporter {

    private var writer: DictionaryBuildingWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit =
      writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)

    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      val start = count
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.encode(os)
          writer.clear()
        }
      }
      Some(count - start)
    }

    override def bytes: Long = 0L

    override def close(): Unit = {
      if (writer != null) {
        if (count % batchSize != 0) {
          writer.encode(os)
          writer.clear()
        }
        writer.close()
      }
    }
  }

  private class BatchDelegate(
      os: OutputStream,
      encoding: SimpleFeatureEncoding,
      batchSize: Int,
      dictionaries: Map[String, ArrowDictionary]
    ) extends FeatureExporter {

    private var writer: SimpleFeatureArrowFileWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit =
      writer = SimpleFeatureArrowFileWriter(sft, os, dictionaries, encoding, None)

    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      val start = count
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.flush()
        }
      }
      Some(count - start)
    }

    override def bytes: Long = 0L

    override def close(): Unit = {
      if (writer != null) {
        writer.close()
      }
    }
  }

  private class SortedBatchDelegate(
      os: OutputStream,
      encoding: SimpleFeatureEncoding,
      sort: (String, Boolean),
      batchSize: Int,
      dictionaries: Map[String, ArrowDictionary]
    ) extends FeatureExporter {

    private val batch = Array.ofDim[SimpleFeature](batchSize)
    private val batches = ArrayBuffer.empty[Array[Byte]]
    private val (sortField, reverse) = sort

    private var vector: SimpleFeatureVector = _
    private var batchWriter: RecordBatchUnloader = _
    private var ordering: Ordering[SimpleFeature] = _

    private var index = 0 // current index into the batch

    override def start(sft: SimpleFeatureType): Unit = {
      vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
      batchWriter = new RecordBatchUnloader(vector)
      ordering = SimpleFeatureOrdering(sft.indexOf(sortField))
      if (reverse) {
        ordering = ordering.reverse
      }
    }

    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      var count = 0L - index
      features.foreach { feature =>
        batch(index) = feature
        index += 1
        if (index % batchSize == 0) {
          sortAndUnloadBatch()
          count += index
          index = 0
        }
      }
      Some(count + index)
    }

    override def bytes: Long = 0L

    override def close(): Unit = {
      if (index > 0) {
        sortAndUnloadBatch()
        index = 0
      }
      val sft = vector.sft
      val bytes = batches.iterator
      val sorted = SimpleFeatureArrowIO.sortBatches(sft, dictionaries, encoding, sortField, reverse, batchSize, bytes)

      WithClose(SimpleFeatureArrowIO.createFile(vector, Some(sortField -> reverse))(sorted))(_.foreach(os.write))
    }

    private def sortAndUnloadBatch(): Unit = {
      java.util.Arrays.sort(batch, 0, index, ordering)
      vector.clear()
      var i = 0
      while (i < index) {
        vector.writer.set(i, batch(i))
        i += 1
      }
      batches.append(batchWriter.unload(index))
    }
  }
}
