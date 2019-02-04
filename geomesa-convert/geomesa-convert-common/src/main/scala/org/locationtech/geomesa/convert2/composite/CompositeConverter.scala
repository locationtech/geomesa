/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.composite

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.{Counter, EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.CompositeEvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.util.Try

class CompositeConverter(val targetSft: SimpleFeatureType, delegates: Seq[(Predicate, SimpleFeatureConverter)])
    extends SimpleFeatureConverter {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

  private val predicates = delegates.mapWithIndex { case ((p, _), i) => (p, i) }.toIndexedSeq
  private val converters = delegates.map(_._2).toIndexedSeq

  override def createEvaluationContext(globalParams: Map[String, Any],
                                       caches: Map[String, EnrichmentCache],
                                       counter: Counter): EvaluationContext = {
    new CompositeEvaluationContext(converters.map(_.createEvaluationContext(globalParams, caches, counter)))
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val setEc: Int => Unit = ec match {
      case c: CompositeEvaluationContext => i => c.setCurrent(i)
      case _ => _ => Unit
    }
    val toEval = Array.ofDim[Any](1)

    def evalPred(pi: (Predicate, Int)): Boolean = {
      setEc(pi._2)
      Try(pi._1.eval(toEval)(ec)).getOrElse(false)
    }

    val lines = IOUtils.lineIterator(is, StandardCharsets.UTF_8)

    new CloseableIterator[SimpleFeature] {

      private var delegate: CloseableIterator[SimpleFeature] = CloseableIterator.empty

      @tailrec
      override def hasNext: Boolean = delegate.hasNext || {
        if (!lines.hasNext) {
          false
        } else {
          toEval(0) = lines.next()
          delegate.close()
          delegate = predicates.find(evalPred).map(_._2) match {
            case None =>
              ec.counter.incLineCount()
              ec.counter.incFailure()
              CloseableIterator.empty

            case Some(i) =>
              val in = new ByteArrayInputStream(toEval(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
              converters(i).process(in, ec)
          }
          hasNext
        }
      }

      override def next(): SimpleFeature = if (hasNext) { delegate.next } else { Iterator.empty.next }

      override def close(): Unit = {
        CloseWithLogging(delegate)
        is.close()
      }
    }
  }

  override def close(): Unit = converters.foreach(CloseWithLogging.apply)
}
