/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics

/**
  * Shared converter state
  */
trait EvaluationContext {

  /**
    * The current line being processed.
    *
    * This may be an actual line (e.g. a csv row), or a logical line (e.g. an avro record)
    *
    * @return
    */
  var line: Long = 0

  /**
    * Enrichment caches
    *
    * @return
    */
  def cache: Map[String, EnrichmentCache]

  /**
    * Metrics registry, accessible for tracking any custom values
    *
    * @return
    */
  def metrics: ConverterMetrics

  /**
    * Counter for tracking successes
    *
    * @return
    */
  def success: com.codahale.metrics.Counter

  /**
    * Counter for tracking failures
    *
    * @return
    */
  def failure: com.codahale.metrics.Counter

  /**
    * Get the current value for the given index
    *
    * @param i value index
    * @return
    */
  def get(i: Int): Any

  /**
    * Set the current value for the given index
    *
    * @param i value index
    * @param value value
    */
  def set(i: Int, value: Any): Unit

  /**
    * Look up an index by name
    *
    * @param name name
    * @return
    */
  def indexOf(name: String): Int

  /**
    * Clear any local (per-entry) state
    */
  def clear(): Unit
}

object EvaluationContext extends LazyLogging {

  val InputFilePathKey = "inputFilePath"

  /**
    * Creates a new, empty evaluation context
    *
    * @return
    */
  def empty: EvaluationContext = new EvaluationContextImpl(Seq.empty, Map.empty, Map.empty, ConverterMetrics.empty)

  /**
    * Creates an evaluation context with the given fields
    *
    * @param localNames names of per-entry fields
    * @param globalValues names and values of global fields
    * @param caches enrichment caches
    * @param metrics metrics
    * @return
    */
  def apply(
      localNames: Seq[String],
      globalValues: Map[String, Any] = Map.empty,
      caches: Map[String, EnrichmentCache] = Map.empty,
      metrics: ConverterMetrics = ConverterMetrics.empty): EvaluationContext = {
    new EvaluationContextImpl(localNames, globalValues, caches, metrics)
  }

  /**
    * Gets a global parameter map containing the input file path
    *
    * @param file input file path
    * @return
    */
  def inputFileParam(file: String): Map[String, AnyRef] = Map(InputFilePathKey -> file)

  /**
    * Evaluation context accessors
    *
    * @param ec context
    */
  implicit class RichEvaluationContext(val ec: EvaluationContext) extends AnyVal {
    def getInputFilePath: Option[String] = ec.indexOf(InputFilePathKey) match {
      case -1 => None
      case i  => Option(ec.get(i)).map(_.toString)
    }
    def setInputFilePath(path: String): Unit = ec.indexOf(InputFilePathKey) match {
      case -1 => throw new IllegalArgumentException(s"$InputFilePathKey is not present in execution context")
      case i  => ec.set(i, path)
    }
  }

  /**
    * Evaluation context implementation
    *
    * @param localNames per-entry variable names
    * @param globalValues global variable name/values (global values are not cleared on `clear`)
    * @param cache enrichment caches
    * @param metrics metrics
    */
  class EvaluationContextImpl(
      localNames: Seq[String],
      globalValues: Map[String, Any],
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics) extends EvaluationContext {

    private val localCount = localNames.length
    // inject the input file path as a global key so there's always a spot for it in the array
    private val names = localNames ++ (globalValues.keys.toSeq :+ EvaluationContext.InputFilePathKey).distinct
    private val values = Array.tabulate[Any](names.length)(i => globalValues.get(names(i)).orNull)

    override val success: com.codahale.metrics.Counter = metrics.counter("success")
    override val failure: com.codahale.metrics.Counter = metrics.counter("failure")

    override def indexOf(name: String): Int = names.indexOf(name)

    override def get(i: Int): Any = values(i)
    override def set(i: Int, value: Any): Unit = values(i) = value

    override def clear(): Unit = {
      var i = 0
      while (i < localCount) {
        values(i) = null
        i += 1
      }
    }
  }

  /**
    * Allows for override of success/failure counters
    *
    * @param delegate delegate context
    * @param success success counter
    * @param failure failure coutner
    */
  class DelegatingEvaluationContext(delegate: EvaluationContext)(
      override val success: com.codahale.metrics.Counter = delegate.success,
      override val failure: com.codahale.metrics.Counter = delegate.failure
    ) extends EvaluationContext {
    override def get(i: Int): Any = delegate.get(i)
    override def set(i: Int, value: Any): Unit = delegate.set(i, value)
    override def indexOf(name: String): Int = delegate.indexOf(name)
    override def clear(): Unit = delegate.clear()
    override def metrics: ConverterMetrics = delegate.metrics
    override def cache: Map[String, EnrichmentCache] = delegate.cache
  }
}
