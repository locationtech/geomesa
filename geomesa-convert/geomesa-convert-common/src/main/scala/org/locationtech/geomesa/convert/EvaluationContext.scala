/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{Counter, Metrics, Tags}
import org.locationtech.geomesa.convert.EvaluationContext.{ContextListener, EvaluationError, FieldAccessor}
import org.locationtech.geomesa.convert2.Field
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics

import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

/**
 * Holds the state associated with a conversion attempt. Evaluation contexts are not thread safe.
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
  @deprecated("Use micrometer global registry for metrics")
  def metrics: ConverterMetrics

  /**
    * Counter for tracking successes
    *
    * @return
    */
  @deprecated("Replaced with `stats`")
  def success: com.codahale.metrics.Counter

  /**
    * Counter for tracking failures
    *
    * @return
    */
  @deprecated("Replaced with `stats`")
  def failure: com.codahale.metrics.Counter

  /**
   * Result stats
   *
   * @return
   */
  def stats: EvaluationContext.Stats

  /**
   * Access to any errors that have occurred - note that errors will generally only be kept if the converter
   * error mode is set to `ReturnErrors`
   *
   * @return
   */
  def errors: java.util.Queue[EvaluationError]

  /**
   * Gets a references to a field's value
   *
   * @param name field name
   * @return
   */
  def accessor(name: String): FieldAccessor

  /**
   * Evaluate all values using the given arguments. The returned array may be mutated on subsequent calls to
   * `evaluate`, so shouldn't be kept long-term
   *
   * @param args single row of input
   */
  def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]]

  /**
   * Returns a new context with a listener attached
   */
  def withListener(listener: ContextListener): EvaluationContext
}

object EvaluationContext extends LazyLogging {

  val InputFilePathKey = "inputFilePath"
  val FilterKey = "filter"

  /**
    * Creates a new, empty evaluation context
    *
    * @return
    */
  def empty: EvaluationContext =
    new StatefulEvaluationContext(Array.empty, Map.empty, Map.empty, ConverterMetrics.empty, Stats())

  /**
   * Creates a new evaluation context with the given state
   *
   * @param fields converter fields, in topological dependency order
   * @param globalValues global values
   * @param caches enrichment caches
   * @param metrics metrics
   * @param success success counter
   * @param failure failure counter
   * @return
   */
  @deprecated("EvaluationContext should be accessed through a converter")
  def apply(
      fields: Seq[Field],
      globalValues: Map[String, _ <: AnyRef],
      caches: Map[String, EnrichmentCache],
      metrics: ConverterMetrics,
      success: com.codahale.metrics.Counter,
      failure: com.codahale.metrics.Counter): EvaluationContext = {
    val stats = Stats.wrap(success, failure, Tags.empty())
    new StatefulEvaluationContext(fields.toArray, globalValues, caches, metrics, stats)
  }

  /**
    * Gets a global parameter map containing the input file path
    *
    * @param file input file path
    * @return
    */
  def inputFileParam(file: String): Map[String, AnyRef] = Map(InputFilePathKey -> file)

  /**
   * Trait for reading a field from an evaluation context
   */
  sealed trait FieldAccessor {
    def apply(): AnyRef
  }

  case object NullFieldAccessor extends FieldAccessor {
    override def apply(): AnyRef = null
  }

  class FieldValueAccessor(values: Array[AnyRef], i: Int) extends FieldAccessor {
    override def apply(): AnyRef = values(i)
  }

  class GlobalFieldAccessor(value: AnyRef) extends FieldAccessor {
    override def apply(): AnyRef = value
  }

  /**
   * Marker trait for resources that are dependent on the evaluation context state
   *
   * @tparam T type
   */
  trait ContextDependent[T <: ContextDependent[T]] {

    /**
     * Return a copy of the instance tied to the given evaluation context.
     *
     * If the instance does not use the evaluation context, it should return itself instead of a copy
     *
     * @param ec evaluation context
     * @return
     */
    def withContext(ec: EvaluationContext): T
  }

  /**
   * Tracks success and failures in the conversion process
   */
  trait Stats {

    /**
     * Increment and retrieve success counts
     *
     * @param i amount to increment (may be zero to just retrieve current value)
     * @return
     */
    def success(i: Int = 1): Long

    /**
     * Increment and retrieve failure counts
     *
     * @param i amount to increment (may be zero to just retrieve current value)
     * @return
     */
    def failure(i: Int = 1): Long
  }

  object Stats {

    /**
     * Create a new stats instance
     *
     * @param tags tags to apply to any metrics
     * @return
     */
    def apply(tags: Tags = Tags.empty()): Stats =
      MicrometerStats(Metrics.counter(ConverterMetrics.name("success"), tags), Metrics.counter(ConverterMetrics.name("failure"), tags))

    /**
     * Wraps dropwizard counters, for back-compatibility
     *
     * @param dwSuccess success
     * @param dwFailure failure
     * @param tags tags
     * @return
     */
    private[geomesa] def wrap(dwSuccess: com.codahale.metrics.Counter, dwFailure: com.codahale.metrics.Counter, tags: Tags): Stats = {
      new Stats {
        private val delegate = Stats(tags) // sets up the micrometer metrics
        override def success(i: Int): Long = { delegate.success(i); dwSuccess.inc(i); dwSuccess.getCount }
        override def failure(i: Int): Long = { delegate.failure(i); dwFailure.inc(i); dwFailure.getCount }
      }
    }

    /**
     * Tracks success and failures in the conversion process. Counters are globally shared, so
     * we track counts locally, while updating the global counters at the same time.
     *
     * Note that micrometer meters (including counters) will not actually store anything unless a registry has been configured.
     *
     * @param success success counter
     * @param failure failure counter
     */
    private case class MicrometerStats(success: Counter, failure: Counter) extends Stats {

      private val localSuccessCount = new AtomicLong(0)
      private val localFailureCount = new AtomicLong(0)

      override def success(i: Int = 1): Long = {
        if (i > 0) {
          success.increment(i)
          localSuccessCount.addAndGet(i)
        } else {
          localSuccessCount.get()
        }
      }

      override def failure(i: Int = 1): Long = {
        if (i > 0) {
          failure.increment(i)
          localFailureCount.addAndGet(i)
        } else {
          localFailureCount.get()
        }
      }
    }
  }

  /**
   * Evaluation error
   *
   * @param field field name that had an error
   * @param line line number of the input being evaluated
   * @param e error
   */
  case class EvaluationError(field: String, line: Long, e: Throwable)

  /**
   * Listener callback trait
   */
  trait ContextListener {

    /**
     * Invoked when success counts change
     *
     * @param i amount of change
     */
    def onSuccess(i: Int): Unit

    /**
     * Invoked when failure counts change
     *
     * @param i amount of change
     */
    def onFailure(i: Int): Unit
  }

  /**
    * Evaluation context accessors
    *
    * @param ec context
    */
  implicit class RichEvaluationContext(val ec: EvaluationContext) extends AnyVal {
    def getInputFilePath: Option[String] = Option(ec.accessor(InputFilePathKey).apply().asInstanceOf[String])
  }

  /**
   * Evaluation context implementation
   *
   * @param fields fields to evaluate, in topological dependency order
   * @param globalValues global variable name/values
   * @param cache enrichment caches
   * @param metrics deprecated metrics
   * @param stats metrics
   * @param errors error tracker
   */
  class StatefulEvaluationContext(
      fields: Array[Field],
      globalValues: Map[String, _ <: AnyRef],
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics,
      val stats: Stats,
      val errors: java.util.Queue[EvaluationError] = new java.util.ArrayDeque[EvaluationError]()
    ) extends EvaluationContext {

    // holder for results from evaluating each row
    private val values = Array.ofDim[AnyRef](fields.length)
    // temp array for holding the arguments for a field
    private val fieldArray = Array.ofDim[AnyRef](1)
    // copy the transforms and tie them to the context
    // note: the class isn't fully instantiated yet, but this statement is last in the initialization
    private val transforms = fields.map(_.transforms.map(_.withContext(this)))

    override lazy val success: com.codahale.metrics.Counter = new com.codahale.metrics.Counter() {
      override def inc(n: Long): Unit = stats.success(n.toInt)
      override def getCount: Long = stats.success(0)
    }
    override lazy val failure: com.codahale.metrics.Counter = new com.codahale.metrics.Counter() {
      override def inc(n: Long): Unit = stats.failure(n.toInt)
      override def getCount: Long = stats.failure(0)
    }

    override def accessor(name: String): FieldAccessor = {
      val i = fields.indexWhere(_.name == name)
      if (i >= 0) { new FieldValueAccessor(values, i) } else {
        globalValues.get(name) match {
          case Some(value) => new GlobalFieldAccessor(value)
          case None => NullFieldAccessor
        }
      }
    }

    override def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]] = {
      var i = 0
      // note: since fields are in topological order we don't need to clear them
      while (i < values.length) {
        values(i) = try {
          val fieldArgs = fields(i).fieldArg match {
            case None => args
            case Some(f) => fieldArray(0) = f.apply(args); fieldArray
          }
          transforms(i) match {
            case Some(t) => t.apply(fieldArgs)
            case None    => fieldArgs(0)
          }
        } catch {
          case NonFatal(e) => return Left(EvaluationError(fields(i).name, line, e))
        }
        i += 1
      }
      Right(values)
    }

    override def withListener(listener: ContextListener): EvaluationContext =
      new StatefulEvaluationContext(fields, globalValues, cache, metrics, StatListener(stats, listener))
  }

  /**
   * Stats implementation that adds a listener.
   *
   * Note that this implementation can chain additional listeners, but it's not very efficient doing so. The
   * typical usage of listeners is reporting out counts, so usually there will only be one.
   *
   * @param delegate delegate stats
   * @param listener listener
   */
  case class StatListener(delegate: Stats, listener: ContextListener) extends Stats {
    override def success(i: Int): Long = { if (i > 0) { listener.onSuccess(i) }; delegate.success(i) }
    override def failure(i: Int): Long = { if (i > 0) { listener.onFailure(i) }; delegate.failure(i) }
  }
}
