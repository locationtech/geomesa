/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.nio.charset.Charset

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.{Counter, EvaluationContext, SimpleFeatureValidator}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.util.control.NonFatal

package object convert2 {

  trait ConverterConfig {
    def `type`: String
    def idField: Option[Expression]
    def caches: Map[String, Config]
    def userData: Map[String, Expression]
  }

  trait Field {
    def name: String
    def transforms: Option[Expression]
    def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = transforms.map(_.eval(args)).getOrElse(args(0))
  }

  trait ConverterOptions {
    def validators: SimpleFeatureValidator
    def parseMode: ParseMode
    def errorMode: ErrorMode
    def encoding: Charset
  }

  /**
    * Handles errors from the underlying parsing of data, before converting to simple features
    *
    * @param underlying wrapped iterator
    * @param mode error mode
    * @param counter counter for failures
    */
  class ErrorHandlingIterator[T](underlying: CloseableIterator[T], mode: ErrorMode, counter: Counter)
      extends CloseableIterator[T] with LazyLogging {

    private var staged: T = _
    private var error = false

    override final def hasNext: Boolean = staged != null || {
      if (error) { false } else {
        // make sure that we successfully read an underlying record, so that we can always return
        // a valid record in `next`, otherwise failures will get double counted
        try {
          if (underlying.hasNext) {
            staged = underlying.next
            true
          } else {
            false
          }
        } catch {
          case NonFatal(e) =>
            counter.incFailure()
            mode match {
              case ErrorMode.SkipBadRecords => logger.warn("Failed parsing input: ", e)
              case ErrorMode.RaiseErrors => throw e
            }
            error = true
            false // usually parsing can't continue if there is an exception in the underlying read
        }
      }
    }

    override def next(): T = {
      if (!hasNext) { Iterator.empty.next() } else {
        val res = staged
        staged = null.asInstanceOf[T]
        res
      }
    }

    override def close(): Unit = underlying.close()
  }
}
