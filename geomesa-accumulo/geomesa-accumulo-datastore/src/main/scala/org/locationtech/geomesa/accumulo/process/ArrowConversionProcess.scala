/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import java.io.{ByteArrayOutputStream, Closeable}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

@DescribeProcess(
  title = "Arrow Conversion",
  description = "Converts a feature collection to arrow format"
)
class ArrowConversionProcess extends VectorProcess with LazyLogging {

  /**
    * Converts an input feature collection to arrow format
    *
    * @param features input features
    * @param encodedAttributes attributes to dictionary encode, optional. Currently only supports String attributes
    * @return
    */
  @DescribeResult(description = "Encoded feature collection")
  def execute(
              @DescribeParameter(name = "features", description = "Input feature collection to query ")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "encodedAttributes", description = "Attributes to dictionary encode", min = 0, max = 128, collectionType = classOf[String])
              encodedAttributes: java.util.List[String]
             ): java.util.Iterator[Array[Byte]] = {

    import scala.collection.JavaConversions._

    logger.debug(s"Running arrow encoding for ${features.getClass.getName}")

    val sft = features.getSchema

    // validate inputs
    val toEncode: Seq[String] = Option(encodedAttributes).map(_.toSeq).getOrElse(Seq.empty)
    toEncode.foreach { attribute =>
      if (sft.indexOf(attribute) == -1) {
        throw new IllegalArgumentException(s"Attribute $attribute doesn't exist in $sft")
      } else if (sft.getDescriptor(attribute).getType.getBinding != classOf[String]) {
        throw new IllegalArgumentException(s"Attribute $attribute is not String type: $sft")
      }
    }

    val visitor = new ArrowVisitor(sft, toEncode)
    features.accepts(visitor, null)
    visitor.close()
    visitor.getResult.results
  }
}

class ArrowVisitor(sft: SimpleFeatureType, encodedAttributes: Seq[String])
    extends FeatureCalc with Closeable with LazyLogging {

  import org.locationtech.geomesa.arrow.allocator

  import scala.collection.JavaConversions._

  // for collecting results manually
  private val manualResults = scala.collection.mutable.Queue.empty[Array[Byte]]
  private val manualBytes = new ByteArrayOutputStream()
  private lazy val manualWriter = new SimpleFeatureArrowFileWriter(sft, manualBytes)
  private var manualVisit = 0L
  private var distributedVisit = false

  private var result = new Iterator[Array[Byte]] {
    override def next(): Array[Byte] = manualResults.dequeue()
    override def hasNext: Boolean = manualResults.nonEmpty
  }

  override def getResult: ArrowResult = ArrowResult(result)

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit = {
    manualWriter.add(feature.asInstanceOf[SimpleFeature])
    manualVisit += 1
    if (manualVisit % 10000 == 0) {
      unloadManualResults(false)
    }
  }

  private def unloadManualResults(finalize: Boolean): Unit = {
    if (finalize) {
      manualWriter.close()
    } else {
      manualWriter.flush()
    }
    manualResults += manualBytes.toByteArray
    manualBytes.reset()
  }

  /**
    * Optimized method to run distributed query. Sets the result, available from `getResult`
    *
    * @param source simple feature source
    * @param query may contain additional filters to apply
    */
  def arrowQuery(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")

    query.getHints.put(QueryHints.ARROW_ENCODE, true)
    query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, encodedAttributes.mkString(","))

    val features = SelfClosingIterator(source.getFeatures(query))
    result = result ++ features.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
    distributedVisit = true
  }

  override def close(): Unit = {
    if (manualVisit > 0 || !distributedVisit) {
      unloadManualResults(true)
    }
  }
}

case class ArrowResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
  override def getValue: AnyRef = results
}
