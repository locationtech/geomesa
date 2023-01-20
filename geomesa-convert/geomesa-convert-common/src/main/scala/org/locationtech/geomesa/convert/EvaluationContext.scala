/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.EvaluationContext.{EvaluationError, FieldAccessor}
import org.locationtech.geomesa.convert2.Field
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics

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
}

object EvaluationContext extends LazyLogging {

  val InputFilePathKey = "inputFilePath"

  /**
    * Creates a new, empty evaluation context
    *
    * @return
    */
  def empty: EvaluationContext = {
    val metrics = ConverterMetrics.empty
    val success = metrics.counter("success")
    val failures = metrics.counter("failure")
    new StatefulEvaluationContext(Array.empty, Map.empty, Map.empty, metrics, success, failures)
  }

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
  def apply(
      fields: Seq[Field],
      globalValues: Map[String, _ <: AnyRef],
      caches: Map[String, EnrichmentCache],
      metrics: ConverterMetrics,
      success: Counter,
      failure: Counter): EvaluationContext = {
    new StatefulEvaluationContext(fields.toArray, globalValues, caches, metrics, success, failure)
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
   * Evaluation error
   *
   * @param field field name that had an error
   * @param line line number of the input being evaluated
   * @param e error
   */
  case class EvaluationError(field: String, line: Long, e: Throwable)

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
    * @param metrics metrics
    */
  class StatefulEvaluationContext(
      fields: Array[Field],
      globalValues: Map[String, _ <: AnyRef],
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics,
      val success: Counter,
      val failure: Counter
    ) extends EvaluationContext {

    // holder for results from evaluating each row
    private val values = Array.ofDim[AnyRef](fields.length)
    // temp array for holding the arguments for a field
    private val fieldArray = Array.ofDim[AnyRef](1)
    // copy the transforms and tie them to the context
    // note: the class isn't fully instantiated yet, but this statement is last in the initialization
    private val transforms = fields.map(_.transforms.map(_.withContext(this)))

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
  }
}
