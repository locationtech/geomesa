/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._

import com.beust.jcommander.ParameterException
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class ArrowExporter(hints: Hints, os: OutputStream, queryDictionaries: => Map[String, Array[AnyRef]])
    extends FeatureExporter {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private var sft: SimpleFeatureType = _

  private var writer: SimpleFeatureArrowFileWriter = _

  private var doExport: Iterator[SimpleFeature] => Option[Long] = _

  override def start(sft: SimpleFeatureType): Unit = {
    this.sft = sft
    if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      doExport = exportEncoded
    } else {
      val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)
      val sort = hints.getArrowSort
      val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)

      if (dictionaryFields.forall(providedDictionaries.contains)) {
        var id = -1
        val dictionaries = (queryDictionaries ++ providedDictionaries).map { case (k, v) =>
          id += 1
          k -> ArrowDictionary.create(id, v)(ClassTag[AnyRef](sft.getDescriptor(k).getType.getBinding))
        }
        writer = SimpleFeatureArrowFileWriter(sft, os, dictionaries, encoding, sort)
        writer.start()
        doExport = exportBatches(encoding, sort, batchSize, dictionaries)
      } else {
        if (sort.isDefined) {
          throw new ParameterException("Sorting and calculating dictionaries at the same time is not supported")
        }
        doExport = exportFiles(dictionaryFields, encoding, batchSize)
      }
    }
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = doExport(features)

  override def close(): Unit = {
    Option(writer).foreach(CloseWithLogging.apply)
    os.close()
  }

  private def exportEncoded(features: Iterator[SimpleFeature]): Option[Long] = {
    // just copy bytes directly out
    features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
    None // we don't know the actual count
  }

  private def exportBatches(encoding: SimpleFeatureEncoding,
                            sort: Option[(String, Boolean)],
                            batchSize: Int,
                            dictionaries: Map[String, ArrowDictionary])
                           (features: Iterator[SimpleFeature]): Option[Long] = {
    if (sort.isDefined) {
      Some(ArrowExporter.writeSortedBatches(sft, encoding, sort.get, dictionaries, batchSize, features, os))
    } else {
      var count = 0L
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.flush()
        }
      }
      if (count % batchSize != 0) {
        writer.flush()
      }
      Some(count)
    }
  }

  private def exportFiles(dictionaryFields: Seq[String],
                          encoding: SimpleFeatureEncoding,
                          batchSize: Int)
                         (features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    WithClose(DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)) { writer =>
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.encode(os)
          writer.clear()
        }
      }
      if (count % batchSize != 0) {
        writer.encode(os)
        writer.clear()
      }
    }
    Some(count)
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

  def writeSortedBatches(sft: SimpleFeatureType,
                         encoding: SimpleFeatureEncoding,
                         sort: (String, Boolean),
                         dictionaries: Map[String, ArrowDictionary],
                         batchSize: Int,
                         features: Iterator[SimpleFeature],
                         out: OutputStream): Long = {
    import SimpleFeatureArrowIO.sortBatches

    val (sortField, reverse) = sort

    val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val batchWriter = new RecordBatchUnloader(vector)

    val ordering = SimpleFeatureOrdering(sft.indexOf(sortField))

    val batches = ArrayBuffer.empty[Array[Byte]]
    val batch = Array.ofDim[SimpleFeature](batchSize)

    var index = 0
    var count = 0L

    def sortAndUnloadBatch(): Unit = {
      java.util.Arrays.sort(batch, 0, index, if (reverse) { ordering.reverse } else { ordering })
      vector.clear()
      var i = 0
      while (i < index) {
        vector.writer.set(i, batch(i))
        i += 1
      }
      batches.append(batchWriter.unload(index))
      count += index
      index = 0
    }

    features.foreach { feature =>
      batch(index) = feature
      index += 1
      if (index % batchSize == 0) {
        sortAndUnloadBatch()
      }
    }

    if (index > 0) {
      sortAndUnloadBatch()
    }

    WithClose(sortBatches(sft, dictionaries, encoding, sortField, reverse, batchSize, batches.iterator)) { sorted =>
      sorted.foreach(out.write)
    }

    count
  }
}