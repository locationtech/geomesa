/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.InputStream

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{Counter, EvaluationContext, Predicate}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.util.Try

class CompositeConverterFactory[I] extends SimpleFeatureConverterFactory[I] {

  override def canProcess(conf: Config): Boolean =
    if (conf.hasPath("type")) conf.getString("type").equals("composite-converter") else false

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val converters: Seq[(Predicate, SimpleFeatureConverter[I])] =
      conf.getConfigList("converters").map { c =>
        val pred = Transformers.parsePred(c.getString("predicate"))
        val converter = SimpleFeatureConverters.build[I](sft, conf.getConfig(c.getString("converter")))
        (pred, converter)
      }
    new CompositeConverter[I](sft, converters)
  }
}

class CompositeConverter[I](val targetSFT: SimpleFeatureType, converters: Seq[(Predicate, SimpleFeatureConverter[I])])
    extends SimpleFeatureConverter[I] {

  val predsWithIndex = converters.map(_._1).zipWithIndex.toIndexedSeq
  val indexedConverters = converters.map(_._2).toIndexedSeq

  override def createEvaluationContext(globalParams: Map[String, Any], counter: Counter): EvaluationContext = {
    val delegates = converters.map(_._2.createEvaluationContext(globalParams, counter)).toIndexedSeq
    new CompositeEvaluationContext(delegates)
  }

  override def processInput(is: Iterator[I], ec: EvaluationContext): Iterator[SimpleFeature] = {
    val setEc: (Int) => Unit = ec match {
      case c: CompositeEvaluationContext => (i) => c.setCurrent(i)
      case _ => (_) => Unit
    }
    val toEval = Array.ofDim[Any](1)

    def evalPred(pi: (Predicate, Int)): Boolean = {
      setEc(pi._2)
      Try(pi._1.eval(toEval)(ec)).getOrElse(false)
    }

    new Iterator[SimpleFeature] {
      var iter: Iterator[SimpleFeature] = loadNext()

      override def hasNext: Boolean = iter.hasNext
      override def next(): SimpleFeature = {
        val res = iter.next()
        if (!iter.hasNext && is.hasNext) {
          iter = loadNext()
        }
        res
      }

      @tailrec
      def loadNext(): Iterator[SimpleFeature] = {
        toEval(0) = is.next()
        val i = predsWithIndex.find(evalPred).map(_._2).getOrElse(-1)
        val res = if (i == -1) {
          ec.counter.incLineCount()
          ec.counter.incFailure()
          Iterator.empty
        } else {
          indexedConverters(i).processInput(Iterator(toEval(0).asInstanceOf[I]), ec)
        }

        if (res.hasNext) {
          res
        } else if (!is.hasNext) {
          Iterator.empty
        } else {
          loadNext()
        }
      }
    }
  }

  override def processSingleInput(i: I, ec: EvaluationContext): Seq[SimpleFeature] = ???

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] = ???
}

case class CompositeEvaluationContext(contexts: IndexedSeq[EvaluationContext]) extends EvaluationContext {

  var current: EvaluationContext = contexts.headOption.orNull

  def setCurrent(i: Int): Unit = current = contexts(i)

  override def get(i: Int): Any = current.get(i)
  override def set(i: Int, v: Any): Unit = current.set(i, v)
  override def indexOf(n: String): Int = current.indexOf(n)
  override def counter: Counter = current.counter
}