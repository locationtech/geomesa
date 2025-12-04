/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.process

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.api.data.{Query, SimpleFeatureSource}
import org.geotools.api.feature.Feature
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.visitor.AbstractCalcResult
import org.locationtech.geomesa.arrow.io.{FormatVersion, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.process.ArrowVisitor.{ArrowManualVisitor, ArrowResult, ComplexArrowManualVisitor, SimpleArrowManualVisitor}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.ByteArrayOutputStream
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

class ArrowVisitor(
    sft: SimpleFeatureType,
    encoding: SimpleFeatureEncoding,
    ipcVersion: String,
    dictionaryFields: Seq[String],
    sortField: Option[String],
    sortReverse: Option[Boolean],
    preSorted: Boolean,
    batchSize: Int,
    flattenStruct: Boolean,
  ) extends GeoMesaProcessVisitor with LazyLogging {

  import scala.collection.JavaConverters._

  // for collecting results manually
  private lazy val manualVisitor: ArrowManualVisitor = {
    val sort = sortField.map(s => (s, sortReverse.getOrElse(false)))
    val ipcOpts = FormatVersion.options(ipcVersion)
    if (dictionaryFields.isEmpty && (sortField.isEmpty || preSorted)) {
      new SimpleArrowManualVisitor(sft, encoding, ipcOpts, sort, batchSize, flattenStruct)
    } else {
      new ComplexArrowManualVisitor(sft, encoding, ipcOpts, dictionaryFields, sort, preSorted, batchSize, flattenStruct)
    }
  }

  private var result: Iterator[Array[Byte]] = _

  override def getResult: ArrowResult = {
    if (result != null) {
      ArrowResult(result.asJava)
    } else {
      ArrowResult(manualVisitor.results.asJava)
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
    query.getHints.put(QueryHints.ARROW_FORMAT_VERSION, ipcVersion)
    query.getHints.put(QueryHints.ARROW_FLATTEN_STRUCT, flattenStruct)
    query.getHints.put(QueryHints.FLIP_AXIS_ORDER, encoding.flipAxisOrder)
    sortField.foreach(query.getHints.put(QueryHints.ARROW_SORT_FIELD, _))
    sortReverse.foreach(query.getHints.put(QueryHints.ARROW_SORT_REVERSE, _))

    val features = CloseableIterator(source.getFeatures(query).features())
    result = features.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
  }
}

object ArrowVisitor {

  private trait ArrowManualVisitor {
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
  private class SimpleArrowManualVisitor(
      sft: SimpleFeatureType,
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      batchSize: Int,
      flattenStruct: Boolean
    ) extends ArrowManualVisitor {

    private val out = new ByteArrayOutputStream()
    private val bytes = ListBuffer.empty[Array[Byte]]
    private var count = 0L

    private val writer =
      SimpleFeatureArrowFileWriter(out, sft, Map.empty[String, ArrowDictionary], encoding, ipcOpts, sort, flattenStruct)

    override def visit(feature: SimpleFeature): Unit = {
      writer.add(feature)
      count += 1
      if (count % batchSize == 0) {
        writer.flush()
        bytes.append(out.toByteArray)
        out.reset()
      }
    }

    override def results: Iterator[Array[Byte]] = {
      CloseWithLogging(writer)
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
  private class ComplexArrowManualVisitor(
      sft: SimpleFeatureType,
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      dictionaryFields: Seq[String],
      sort: Option[(String, Boolean)],
      preSorted: Boolean,
      batchSize: Int,
      flattenStruct: Boolean
    ) extends ArrowManualVisitor {

    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConverters._

    private val features = ArrayBuffer.empty[SimpleFeature]

    override def visit(feature: SimpleFeature): Unit = {
      // copy the feature in case it is being re-used in the iterator
      features.append(ScalaSimpleFeature.copy(feature))
    }

    override def results: Iterator[Array[Byte]] = {
      val dictionaries = dictionaryFields.map { field =>
        val i = sft.indexOf(field)
        val descriptor = sft.getDescriptor(i)
        val isList = descriptor.isList
        val values = scala.collection.mutable.HashSet.empty[AnyRef]
        features.foreach { f =>
          val value = f.getAttribute(i)
          if (value != null) {
            if (isList) {
              // for list types encode the list elements instead of the list itself
              value.asInstanceOf[java.util.List[AnyRef]].asScala.foreach(values.add)
            } else {
              values.add(value)
            }
          }
        }
        val binding = if (isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
        field -> ArrowDictionary.create(sft.getTypeName, i, values.toArray)(ClassTag[AnyRef](binding))
      }

      val sorted = sort match {
        case Some((field, reverse)) if !preSorted => features.sorted(SimpleFeatureOrdering(sft, field, reverse)).iterator
        case _ => features.iterator
      }

      val out = new ByteArrayOutputStream()
      val bytes = ListBuffer.empty[Array[Byte]]

      WithClose(SimpleFeatureArrowFileWriter(out, sft, dictionaries.toMap, encoding, ipcOpts, sort, flattenStruct)) { writer =>
        while (sorted.hasNext) { // send batches
          var i = 0
          while (i < batchSize && sorted.hasNext) {
            writer.add(sorted.next)
            i += 1
          }
          writer.flush()
          bytes.append(out.toByteArray)
          out.reset()
        }
      }
      bytes.append(out.toByteArray)
      bytes.iterator
    }
  }

  case class ArrowResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
    override def getValue: AnyRef = results
  }
}
