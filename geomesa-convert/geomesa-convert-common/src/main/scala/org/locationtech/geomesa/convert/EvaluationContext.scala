/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.EvaluationContext.{EvaluationError, FieldAccessor}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.convert2.AbstractConverter.AbstractApiError
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.convert2.AbstractConverter.AbstractApiError
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======

  /**
   * Evaluate all values using the given arguments. The returned array may be mutated on subsequent calls to
   * `evaluate`, so shouldn't be kept long-term
   *
   * @param args single row of input
   */
  def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]]

  /**
    * Get the current value for the given index
    *
    * @param i value index
    * @return
    */
  @deprecated("Replaced with `accessor`, indices are not stable between evaluation contexts")
  def get(i: Int): Any

  /**
    * Set the current value for the given index
    *
    * @param i value index
    * @param value value
    */
  @deprecated("Evaluation contexts should not be mutated")
  def set(i: Int, value: Any): Unit

  /**
    * Look up an index by name
    *
    * @param name name
    * @return
    */
  @deprecated("Replaced with `accessor`, indices are not stable between evaluation contexts")
  def indexOf(name: String): Int

  /**
    * Clear any local (per-entry) state
    */
  @deprecated("Evaluation contexts should not be mutated")
  def clear(): Unit
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
  @deprecated("Replaced with `apply(fields)`")
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
  def apply(
      fields: Seq[Field],
      globalValues: Map[String, _ <: AnyRef],
      caches: Map[String, EnrichmentCache],
      metrics: ConverterMetrics,
      success: Counter,
      failure: Counter): EvaluationContext = {
    new StatefulEvaluationContext(fields.toArray, globalValues, caches, metrics, success, failure)
  }

<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    // noinspection ScalaDeprecation
    @deprecated("Evaluation contexts should not be mutated")
    def setInputFilePath(path: String): Unit = ec.indexOf(InputFilePathKey) match {
      case -1 => throw new IllegalArgumentException(s"$InputFilePathKey is not present in execution context")
      case i  => ec.set(i, path)
    }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics,
      val success: Counter,
      val failure: Counter
<<<<<<< HEAD
=======
    ) extends EvaluationContext {

    // holder for results from evaluating each row
    private val values = Array.ofDim[AnyRef](fields.length)
    // temp array for holding the arguments for a field
    private val fieldArray = Array.ofDim[AnyRef](1)
    // copy the transforms and tie them to the context
    // note: the class isn't fully instantiated yet, but this statement is last in the initialization
    private val transforms = fields.map(_.transforms.map(_.withContext(this)))

    // to support the deprecated `get` and `set` methods
    private lazy val sortedGlobalValues = globalValues.toArray.sortBy(_._1)

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
        try {
          val value = try {
            val fieldArgs = fields(i).fieldArg match {
              case None => args
              case Some(f) => fieldArray(0) = f.apply(args); fieldArray
            }
            transforms(i) match {
              case Some(t) => t.apply(fieldArgs)
              case None    => fieldArgs(0)
            }
          } catch {
            case _: AbstractApiError =>
              // back-compatible shim for fields that haven't been updated
              // noinspection ScalaDeprecation
              fields(i).eval(args.asInstanceOf[Array[Any]])(this).asInstanceOf[AnyRef]
          }
          values(i) = value
        } catch {
          case NonFatal(e) => return Left(EvaluationError(fields(i).name, line, e))
        }
        i += 1
      }
      Right(values)
    }

    // noinspection ScalaDeprecation
    override def get(i: Int): Any = {
      if (i < values.length) {
        values(i)
      } else if (i - values.length < sortedGlobalValues.length) {
        sortedGlobalValues(i - values.length)._2
      } else {
        null
      }
    }

    // noinspection ScalaDeprecation
    override def set(i: Int, value: Any): Unit = {
      if (i < values.length) {
        values(i) = value.asInstanceOf[AnyRef]
      } else if (i - values.length < sortedGlobalValues.length) {
        sortedGlobalValues(i - values.length) = sortedGlobalValues(i - values.length)._1 -> value.asInstanceOf[AnyRef]
      }
    }

    // noinspection ScalaDeprecation
    override def indexOf(name: String): Int = {
      val local = fields.indexWhere(_.name == name)
      if (local != -1) { local } else {
        val global = sortedGlobalValues.indexWhere(_._1 == name)
        if (global == -1) { -1 } else { global + fields.length }
      }
    }

    // noinspection ScalaDeprecation
    override def clear(): Unit = {
      var i = 0
      while (i < values.length) {
        values(i) = null
        i += 1
      }
    }
  }

  @deprecated("replaced with StatefulEvaluationContext")
  class EvaluationContextImpl(
      localNames: Seq[String],
      globalValues: Map[String, Any],
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

    // noinspection ScalaDeprecation
    override def get(i: Int): Any = {
      if (i < values.length) {
        values(i)
      } else if (i - values.length < sortedGlobalValues.length) {
        sortedGlobalValues(i - values.length)._2
      } else {
        null
      }
    }

    // noinspection ScalaDeprecation
    override def set(i: Int, value: Any): Unit = {
      if (i < values.length) {
        values(i) = value.asInstanceOf[AnyRef]
      } else if (i - values.length < sortedGlobalValues.length) {
        sortedGlobalValues(i - values.length) = sortedGlobalValues(i - values.length)._1 -> value.asInstanceOf[AnyRef]
      }
    }

    // noinspection ScalaDeprecation
    override def indexOf(name: String): Int = {
      val local = fields.indexWhere(_.name == name)
      if (local != -1) { local } else {
        val global = sortedGlobalValues.indexWhere(_._1 == name)
        if (global == -1) { -1 } else { global + fields.length }
      }
    }

    // noinspection ScalaDeprecation
    override def clear(): Unit = {
      var i = 0
      while (i < values.length) {
        values(i) = null
        i += 1
      }
    }
  }

  @deprecated("replaced with StatefulEvaluationContext")
  class EvaluationContextImpl(
      localNames: Seq[String],
      globalValues: Map[String, Any],
      val cache: Map[String, EnrichmentCache],
      val metrics: ConverterMetrics
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======

    override def accessor(name: String): FieldAccessor =
      new FieldValueAccessor(values.asInstanceOf[Array[AnyRef]], indexOf(name))

    override def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]] =
      throw new NotImplementedError()
  }

  /**
    * Allows for override of success/failure counters
    *
    * @param delegate delegate context
    * @param success success counter
    * @param failure failure coutner
    */
  // noinspection ScalaDeprecation
  @deprecated("Does not delegate line number correctly")
  class DelegatingEvaluationContext(delegate: EvaluationContext)(
      override val success: com.codahale.metrics.Counter = delegate.success,
      override val failure: com.codahale.metrics.Counter = delegate.failure
    ) extends EvaluationContext {
    override def accessor(name: String): FieldAccessor = delegate.accessor(name)
    override def evaluate(args: Array[AnyRef]): Either[EvaluationError, Array[AnyRef]] = delegate.evaluate(args)
    override def metrics: ConverterMetrics = delegate.metrics
    override def cache: Map[String, EnrichmentCache] = delegate.cache
    override def get(i: Int): Any = delegate.get(i)
    override def set(i: Int, value: Any): Unit = delegate.set(i, value)
    override def indexOf(name: String): Int = delegate.indexOf(name)
    override def clear(): Unit = delegate.clear()
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
  }
}
