/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.{Closeable, OutputStream}

import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator

class ArrowExporter(ds: GeoMesaDataStore[_, _, _], query: Query, os: OutputStream) extends FeatureExporter {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private var writer: Closeable = _

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    val sft = fc.getSchema
    val features = SelfClosingIterator(fc.features())
    if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      // just copy bytes directly out
      features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
      None // we don't know the actual count
    } else {
      val hints = query.getHints
      val includeFid = hints.isArrowIncludeFid
      val batchSize = hints.getArrowBatchSize.getOrElse(1000000)
      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues
      var count = 0L
      if (hints.isArrowComputeDictionaries || dictionaryFields.forall(providedDictionaries.contains)) {
        val dictionaries: Map[String, ArrowDictionary] = if (dictionaryFields.isEmpty) { Map.empty } else {
          import scala.collection.JavaConversions._
          val provided = providedDictionaries.map { case (k, v) => k -> ArrowDictionary.create(v) }
          val queried = {
            val remaining = dictionaryFields.filterNot(providedDictionaries.contains)
            if (remaining.isEmpty) { Map.empty } else {
              val dictionaryQuery = new Query(query.getTypeName, query.getFilter)
              dictionaryQuery.setPropertyNames(remaining)
              val map = remaining.map(f => f -> scala.collection.mutable.HashSet.empty[AnyRef]).toMap
              SelfClosingIterator(ds.getFeatureReader(dictionaryQuery, Transaction.AUTO_COMMIT)).foreach { sf =>
                map.foreach { case (k, values) => Option(sf.getAttribute(k)).foreach(values.add) }
              }
              map.map { case (k, s) => k -> ArrowDictionary.create(s.toSeq) }
            }
          }
          provided ++ queried
        }
        val writer = new SimpleFeatureArrowFileWriter(sft, os, dictionaries, includeFid, GeometryPrecision.Float)
        this.writer = writer
        writer.start()
        features.foreach { f =>
          writer.add(f)
          count += 1
          if (count % batchSize == 0) {
            writer.flush()
          }
        }
      } else {
        val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, includeFid, GeometryPrecision.Float)
        this.writer = writer
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

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
    os.close()
  }
}
