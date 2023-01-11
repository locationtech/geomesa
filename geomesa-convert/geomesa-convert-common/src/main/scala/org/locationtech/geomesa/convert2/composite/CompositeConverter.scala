/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.composite

import com.codahale.metrics.Counter
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractCompositeConverter.{CompositeEvaluationContext, PredicateContext}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.util.Try

class CompositeConverter(val targetSft: SimpleFeatureType, delegates: Seq[(Predicate, SimpleFeatureConverter)])
    extends SimpleFeatureConverter {

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    CompositeEvaluationContext(delegates.map(_._2), globalParams)

  override def createEvaluationContext(
      globalParams: Map[String, Any],
      success: Counter,
      failure: Counter): EvaluationContext = {
    CompositeEvaluationContext(delegates.map(_._2), globalParams, success, failure)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val contexts = ec match {
      case cec: CompositeEvaluationContext => cec.contexts.iterator
      case _ => Iterator.continually(ec)
    }
    val predicates = delegates.map { case (predicate, converter) =>
      if (!contexts.hasNext) {
        throw new IllegalArgumentException(s"Invalid evaluation context doesn't match this converter: $ec")
      }
      val context = contexts.next()
      PredicateContext(predicate.withContext(context), converter, context)
    }

    val toEval = Array.ofDim[AnyRef](1)

    val lines = IOUtils.lineIterator(is, StandardCharsets.UTF_8)

    new CloseableIterator[SimpleFeature] {

      private var delegate: CloseableIterator[SimpleFeature] = CloseableIterator.empty

      @tailrec
      override def hasNext: Boolean = delegate.hasNext || {
        if (!lines.hasNext) {
          false
        } else {
          toEval(0) = lines.next()
          ec.line += 1
          delegate.close()
          delegate = null
          lazy val in = new ByteArrayInputStream(toEval(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
          predicates.foreach { p =>
            if (delegate == null && Try(p.predicate(toEval)).getOrElse(false)) {
              delegate = p.converter.process(in, p.context)
            } else {
              p.context.line += 1
            }
          }
          if (delegate == null) {
            ec.failure.inc()
            delegate = CloseableIterator.empty
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

  override def close(): Unit = CloseWithLogging(delegates.map(_._2))
}
