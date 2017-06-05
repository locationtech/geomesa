/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._

import com.beust.jcommander.ParameterException
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

class ArrowExporter(hints: Hints, os: OutputStream, queryDictionaries: => Map[String, Seq[AnyRef]])
    extends FeatureExporter {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    val sft = fc.getSchema
    val features = SelfClosingIterator(fc.features())
    if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      // just copy bytes directly out
      features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
      None // we don't know the actual count
    } else {
      val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)
      val sort = hints.getArrowSort
      val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
      val dictionaryFields = hints.getArrowDictionaryFields

      if (hints.isArrowComputeDictionaries || dictionaryFields.isEmpty) {
        val dictionaries = (queryDictionaries ++ hints.getArrowDictionaryEncodedValues).map {
          case (k, v) => k -> ArrowDictionary.create(v)
        }
        WithClose(new SimpleFeatureArrowFileWriter(sft, os, dictionaries, encoding, sort)) { writer =>
          writer.start()
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
            Some(count)
          }
        }
      } else {
        if (sort.isDefined) {
          throw new ParameterException("Sorting and calculating dictionaries at the same time is not supported")
        }
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
        }
        Some(count)
      }
    }
  }

  override def close(): Unit = os.close()
}

object ArrowExporter {

  import org.locationtech.geomesa.arrow.allocator

  def queryDictionaries(ds: GeoMesaDataStore[_, _, _], query: Query): Map[String, Seq[AnyRef]] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConversions._

    val hints = query.getHints
    val dictionaryFields = {
      val provided = hints.getArrowDictionaryEncodedValues
      hints.getArrowDictionaryFields.filterNot(provided.contains)
    }

    if (dictionaryFields.isEmpty || !hints.isArrowComputeDictionaries) { Map.empty } else {
      val dictionaryQuery = new Query(query.getTypeName, query.getFilter)
      dictionaryQuery.setPropertyNames(dictionaryFields)
      val map = dictionaryFields.map(f => f -> scala.collection.mutable.HashSet.empty[AnyRef]).toMap
      SelfClosingIterator(ds.getFeatureReader(dictionaryQuery, Transaction.AUTO_COMMIT)).foreach { sf =>
        map.foreach { case (k, values) => Option(sf.getAttribute(k)).foreach(values.add) }
      }
      map.map { case (k, values) => (k, values.toSeq) }
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

    val ordering = new Ordering[SimpleFeature] {
      override def compare(x: SimpleFeature, y: SimpleFeature): Int = {
        val left = x.getAttribute(sortField).asInstanceOf[Comparable[Any]]
        val right = y.getAttribute(sortField).asInstanceOf[Comparable[Any]]
        left.compareTo(right)
      }
    }

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