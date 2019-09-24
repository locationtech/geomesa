/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.io.ByteArrayOutputStream

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.process.transform.ArrowConversionProcess.ArrowVisitor
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

@DescribeProcess(
  title = "Arrow Conversion",
  description = "Converts a feature collection to arrow format"
)
class ArrowConversionProcess extends GeoMesaProcess with LazyLogging {

  /**
    * Converts an input feature collection to arrow format
    *
    * @param features input features
    * @param dictionaryFields attributes to dictionary encode, optional
    * @return
    */
  @DescribeResult(description = "Encoded feature collection")
  def execute(
              @DescribeParameter(name = "features", description = "Input feature collection to encode")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "includeFids", description = "Include feature IDs in arrow file", min = 0)
              includeFids: java.lang.Boolean,
              @DescribeParameter(name = "proxyFids", description = "Proxy feature IDs to ints instead of strings", min = 0)
              proxyFids: java.lang.Boolean,
              @DescribeParameter(name = "dictionaryFields", description = "Attributes to dictionary encode", min = 0, max = 128, collectionType = classOf[String])
              dictionaryFields: java.util.List[String],
              @DescribeParameter(name = "useCachedDictionaries", description = "Use cached top-k stats (if available), or run a dynamic stats query to build dictionaries", min = 0)
              useCachedDictionaries: java.lang.Boolean,
              @DescribeParameter(name = "sortField", description = "Attribute to sort by", min = 0)
              sortField: String,
              @DescribeParameter(name = "sortReverse", description = "Reverse the default sort order", min = 0)
              sortReverse: java.lang.Boolean,
              @DescribeParameter(name = "batchSize", description = "Number of features to include in each record batch", min = 0)
              batchSize: java.lang.Integer,
              @DescribeParameter(name = "doublePass", description = "Build dictionaries first, then query results in a separate scan", min = 0)
              doublePass: java.lang.Boolean
             ): java.util.Iterator[Array[Byte]] = {

    import scala.collection.JavaConversions._

    logger.debug(s"Running arrow encoding for ${features.getClass.getName}")

    val sft = features.getSchema

    // validate inputs
    val toEncode: Seq[String] = Option(dictionaryFields).map(_.toSeq).getOrElse(Seq.empty)
    toEncode.foreach { attribute =>
      if (sft.indexOf(attribute) == -1) {
        throw new IllegalArgumentException(s"Attribute $attribute doesn't exist in $sft")
      }
    }
    val cacheDictionaries = Option(useCachedDictionaries).map(_.booleanValue())

    val encoding = SimpleFeatureEncoding.min(includeFids == null || includeFids, proxyFids != null && proxyFids)
    val reverse = Option(sortReverse).map(_.booleanValue())
    val batch = Option(batchSize).map(_.intValue).getOrElse(ArrowProperties.BatchSize.get.toInt)
    val double = Option(doublePass).exists(_.booleanValue())

    val visitor =
      new ArrowVisitor(sft, encoding, toEncode, cacheDictionaries, Option(sortField), reverse, false, batch, double)
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}

object ArrowConversionProcess {

  class ArrowVisitor(sft: SimpleFeatureType,
                     encoding: SimpleFeatureEncoding,
                     dictionaryFields: Seq[String],
                     cacheDictionaries: Option[Boolean],
                     sortField: Option[String],
                     sortReverse: Option[Boolean],
                     preSorted: Boolean,
                     batchSize: Int,
                     doublePass: Boolean)
      extends GeoMesaProcessVisitor with LazyLogging {

    import scala.collection.JavaConversions._

    // for collecting results manually
    private lazy val manualVisitor: ArrowManualVisitor = {
      val sort = sortField.map(s => (s, sortReverse.getOrElse(false)))
      if (dictionaryFields.isEmpty && (sortField.isEmpty || preSorted)) {
        new SimpleArrowManualVisitor(sft, encoding, sort, batchSize)
      } else {
        new ComplexArrowManualVisitor(sft, encoding, dictionaryFields, sort, preSorted, batchSize)
      }
    }

    private var result: Iterator[Array[Byte]] = _

    override def getResult: ArrowResult = {
      if (result != null) {
        ArrowResult(result)
      } else {
        ArrowResult(manualVisitor.results)
      }
    }

    // manually called for non-accumulo feature collections
    override def visit(feature: Feature): Unit = manualVisitor.visit(feature.asInstanceOf[SimpleFeature])

    /**
      * Optimized method to run distributed query. Sets the result, available from `getResult`
      *
      * @param source simple feature source
      * @param query may contain additional filters to apply
      */
    override def execute(source: SimpleFeatureSource, query: Query): Unit = {
      logger.debug(s"Visiting source type: ${source.getClass.getName}")

      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, dictionaryFields.mkString(","))
      query.getHints.put(QueryHints.ARROW_INCLUDE_FID, encoding.fids.isDefined)
      query.getHints.put(QueryHints.ARROW_PROXY_FID, encoding.fids.contains(Encoding.Min))
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, batchSize)
      query.getHints.put(QueryHints.ARROW_DOUBLE_PASS, doublePass)
      cacheDictionaries.foreach(query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, _))
      sortField.foreach(query.getHints.put(QueryHints.ARROW_SORT_FIELD, _))
      sortReverse.foreach(query.getHints.put(QueryHints.ARROW_SORT_REVERSE, _))

      val features = SelfClosingIterator(source.getFeatures(query))
      result = features.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
    }
  }

  trait ArrowManualVisitor {
    def visit(feature: SimpleFeature): Unit
    def results: Iterator[Array[Byte]]
  }

  /**
    * Writes out features in batches, without sorting or dictionaries
    *
    * @param sft simple feature type
    * @param encoding arrow encoding
    * @param sort sort field, only used for metadata - no sorting will be done
    * @param batchSize batch size
    */
  private class SimpleArrowManualVisitor(sft: SimpleFeatureType,
                                         encoding: SimpleFeatureEncoding,
                                         sort: Option[(String, Boolean)],
                                         batchSize: Int)
      extends ArrowManualVisitor {

    import org.locationtech.geomesa.arrow.allocator

    private val out = new ByteArrayOutputStream()
    private val bytes = ListBuffer.empty[Array[Byte]]
    private var count = 0L

    private val writer = SimpleFeatureArrowFileWriter(sft, out, Map.empty, encoding, sort)

    override def visit(feature: SimpleFeature): Unit = {
      writer.add(feature.asInstanceOf[SimpleFeature])
      count += 1
      if (count % batchSize == 0) {
        writer.flush()
        bytes.append(out.toByteArray)
        out.reset()
      }
    }

    override def results: Iterator[Array[Byte]] = {
      writer.close()
      bytes.append(out.toByteArray)
      bytes.iterator
    }
  }

  /**
    * Caches features locally in order to compute dictionaries and/or sorting
    *
    * @param sft simple feature type
    * @param encoding arrow encoding
    * @param dictionaryFields dictionary fields
    * @param sort sort
    * @param batchSize batch size
    */
  private class ComplexArrowManualVisitor(sft: SimpleFeatureType,
                                          encoding: SimpleFeatureEncoding,
                                          dictionaryFields: Seq[String],
                                          sort: Option[(String, Boolean)],
                                          preSorted: Boolean,
                                          batchSize: Int) extends ArrowManualVisitor {

    import org.locationtech.geomesa.arrow.allocator

    private val features = ArrayBuffer.empty[SimpleFeature]

    override def visit(feature: SimpleFeature): Unit = {
      // copy the feature in case it is being re-used in the iterator
      features.append(ScalaSimpleFeature.copy(feature))
    }

    override def results: Iterator[Array[Byte]] = {
      val dictionaries = if (dictionaryFields.isEmpty) { Map.empty[String, ArrowDictionary] } else {
        val indicesAndValues = dictionaryFields.map { field =>
          (field, sft.indexOf(field), scala.collection.mutable.HashSet.empty[AnyRef])
        }
        features.foreach { f =>
          indicesAndValues.foreach { case (_, i, v) => v.add(f.getAttribute(i)) }
        }
        indicesAndValues.map { case (n, i, v) =>
          n -> ArrowDictionary.create(i, v.toArray)(ClassTag[AnyRef](sft.getDescriptor(i).getType.getBinding))
        }.toMap
      }

      val ordering = if (preSorted) { None } else {
        sort.map { case (field, reverse) =>
          val o = SimpleFeatureOrdering(sft.indexOf(field))
          if (reverse) { o.reverse } else { o }
        }
      }
      val sorted = ordering match {
        case None    => features.iterator
        case Some(o) => features.sorted(o).iterator
      }

      val out = new ByteArrayOutputStream()
      val writer = SimpleFeatureArrowFileWriter(sft, out, dictionaries, encoding, sort)

      new Iterator[Array[Byte]] {
        override def hasNext: Boolean = sorted.hasNext
        override def next(): Array[Byte] = {
          out.reset()
          var i = 0
          while (i < batchSize && sorted.hasNext) {
            writer.add(sorted.next)
            i += 1
          }
          if (sorted.hasNext) {
            writer.flush()
          } else {
            writer.close()
          }
          out.toByteArray
        }
      }
    }
  }

  case class ArrowResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
    override def getValue: AnyRef = results
  }
}
