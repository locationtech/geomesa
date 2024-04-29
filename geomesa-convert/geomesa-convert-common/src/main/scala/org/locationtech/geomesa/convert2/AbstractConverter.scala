/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.codahale.metrics.{Counter, Histogram}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.convert.EvaluationContext.EvaluationError
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.{EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, addDependencies, topologicalOrder}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics.SimpleGauge
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.validators.SimpleFeatureValidator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.{IOException, InputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Date
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Abstract converter implementation. Typically paired with an AbstractConverterFactory. If so, needs to have
  * a default constructor consisting of (targetSft, config, fields, options), so that the AbstractConverterFactory
  * can instantiate it via reflection.
  *
  * Subclasses need to implement `read` to parse the underlying input stream into raw values that will be
  * transformed to simple features.
  *
  * @param sft simple feature type
  * @param config converter config
  * @param fields converter fields
  * @param options converter options
  * @tparam T intermediate parsed values binding
  * @tparam C config binding
  * @tparam F field binding
  * @tparam O options binding
  */
abstract class AbstractConverter[T, C <: ConverterConfig, F <: Field, O <: ConverterOptions]
  (val sft: SimpleFeatureType, val config: C, val fields: Seq[F], val options: O)
    extends SimpleFeatureConverter with ParsingConverter[T] with LazyLogging {

  import AbstractConverter.{IdFieldName, UserDataFieldPrefix}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  if (fields.exists(f => f.name == IdFieldName || f.name.startsWith(UserDataFieldPrefix))) {
    throw new IllegalArgumentException(
      s"Field name(s) conflict with reserved values $IdFieldName and/or $UserDataFieldPrefix")
  }

  private val requiredFields: Array[Field] = {
    val fieldNameMap = fields.map(f => f.name -> f.asInstanceOf[Field]).toMap
    val dag = scala.collection.mutable.Map.empty[Field, Set[Field]]

    // compute only the input fields that we need to deal with to populate the simple feature
    sft.getAttributeDescriptors.asScala.foreach { ad =>
      fieldNameMap.get(ad.getLocalName).foreach(addDependencies(_, fieldNameMap, dag))
    }

    // add id field and user data
    config.idField.foreach { expression =>
      addDependencies(BasicField(IdFieldName, Some(expression)), fieldNameMap, dag)
    }
    config.userData.foreach { case (key, expression) =>
      addDependencies(BasicField(UserDataFieldPrefix + key, Some(expression)), fieldNameMap, dag)
    }

    // use a topological ordering to ensure that dependencies are evaluated before the fields that require them
    val ordered = topologicalOrder(dag)

    // log warnings for missing/unused fields
    val used = ordered.map(_.name)
    val undefined = sft.getAttributeDescriptors.asScala.map(_.getLocalName).diff(used)
    if (undefined.nonEmpty) {
      logger.warn(
        s"'${sft.getTypeName}' converter did not define fields for some attributes: ${undefined.mkString(", ")}")
    }
    val unused = fields.map(_.name).diff(used)
    if (unused.nonEmpty) {
      logger.warn(s"'${sft.getTypeName}' converter defined unused fields: ${unused.mkString(", ")}")
    }

    ordered
  }

  private val attributeIndices: Array[(Int, Int)] = {
    val builder = Array.newBuilder[(Int, Int)]
    builder.sizeHint(sft.getAttributeCount)
    var i = 0
    while (i < sft.getAttributeCount) {
      val d = sft.getDescriptor(i)
      // note: missing fields are already checked and logged in requiredFields
      val j = requiredFields.indexWhere(_.name == d.getLocalName)
      if (j != -1) {
        builder += i -> j
      }
      i += 1
    }
    builder.result()
  }


  private val idIndex: Int = requiredFields.indexWhere(_.name == IdFieldName)

  private val userDataIndices: Array[(String, Int)] = {
    val builder = Array.newBuilder[(String, Int)]
    builder.sizeHint(requiredFields.count(_.name.startsWith(UserDataFieldPrefix)))
    var i = 0
    while (i < requiredFields.length) {
      if (requiredFields(i).name.startsWith(UserDataFieldPrefix)) {
        builder += requiredFields(i).name.substring(UserDataFieldPrefix.length) -> i
      }
      i += 1
    }
    builder.result
  }

  private val metrics = ConverterMetrics(sft, options.reporters)

  private val validators = SimpleFeatureValidator(sft, options.validators, metrics, idIndex != -1)

  private val caches = config.caches.map { case (k, v) => (k, EnrichmentCache(v)) }

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    createEvaluationContext(globalParams, metrics.counter("success"), metrics.counter("failure"))

  override def createEvaluationContext(
      globalParams: Map[String, Any],
      success: Counter,
      failure: Counter): EvaluationContext = {
    EvaluationContext(requiredFields, globalParams.asInstanceOf[Map[String, AnyRef]], caches, metrics, success, failure)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val hist = ec.metrics.histogram("parse.nanos")
    val converted = convert(new ErrorHandlingIterator(parse(is, ec), options.errorMode, ec, hist), ec)
    options.parseMode match {
      case ParseMode.Incremental => converted
      case ParseMode.Batch => CloseableIterator((new ListBuffer() ++= converted).iterator, converted.close())
    }
  }

  override def convert(values: CloseableIterator[T], ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val rate = ec.metrics.meter("rate")
    val duration = ec.metrics.histogram("convert.nanos")
    val dtgMetrics = sft.getDtgIndex.map { i =>
      (i, ec.metrics.gauge[Date]("dtg.last"), ec.metrics.histogram("dtg.latency.millis"))
    }

    this.values(values, ec).flatMap { raw =>
      rate.mark()
      val start = System.nanoTime()
      try { convert(raw.asInstanceOf[Array[AnyRef]], ec, dtgMetrics) } finally {
        duration.update(System.nanoTime() - start)
      }
    }
  }

  /**
    * Parse objects out of the input stream. This should be lazily evaluated, so that any exceptions occur
    * in the call to `hasNext` (and not during the iterator creation), which lets us handle them appropriately
    * in `ErrorHandlingIterator`. If there is any sense of 'lines', they should be indicated with
    * `ec.counter.incLineCount`
    *
    * @param is input
    * @param ec evaluation context
    * @return raw extracted values, one iterator entry per simple feature
    */
  protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[T]

  /**
    * Convert parsed values into the raw array used for reading fields. If line counting was not handled
    * in `parse` (due to no sense of 'lines' in the input), then the line count should be incremented here
    * instead
    *
    * @param parsed parsed values
    * @param ec evaluation context
    * @return
    */
  protected def values(parsed: CloseableIterator[T], ec: EvaluationContext): CloseableIterator[Array[Any]]

  /**
    * Convert input values into a simple feature with attributes.
    *
    * This method returns a CloseableIterator to simplify flatMapping over inputs, but it will always
    * return either 0 or 1 feature.
    *
    * @param rawValues raw input values
    * @param ec evaluation context
    * @return
    */
  private def convert(
      rawValues: Array[AnyRef],
      ec: EvaluationContext,
      dtgMetrics: Option[(Int, SimpleGauge[Date], Histogram)]): CloseableIterator[SimpleFeature] = {

    val result = ec.evaluate(rawValues).right.flatMap { values =>
      val sf = new ScalaSimpleFeature(sft, "")

      attributeIndices.foreach { case (i, j) =>
        sf.setAttributeNoConvert(i, values(j))
      }
      // if no id builder, empty feature id will be replaced with an auto-gen one
      if (idIndex != -1) {
        values(idIndex) match {
          case id: String =>
            sf.setId(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

          case null => // no-op

          case id =>
            // the missing feature id will be caught by the validator
            logger.warn(s"Ignoring non-string feature ID: $id")
        }
      }
      userDataIndices.foreach { case (k, j) =>
        sf.getUserData.put(k, values(j))
      }
      val error = validators.validate(sf)
      if (error == null) {
        Right(sf)
      } else {
        Left(EvaluationError(null, ec.line, new IOException(s"Validation error: $error")))
      }
    }

    result match {
      case Right(feature) =>
        ec.success.inc()
        dtgMetrics.foreach { case (index, dtg, latency) =>
          val date = feature.getAttribute(index).asInstanceOf[Date]
          if (date != null) {
            dtg.set(date)
            latency.update(System.currentTimeMillis() - date.getTime)
          }
        }
        CloseableIterator.single(feature)

      case Left(error) =>
        ec.failure.inc()
        def msg(verbose: Boolean): String = errorMessage(error, rawValues, verbose)
        options.errorMode match {
          case ErrorMode.LogErrors if logger.underlying.isDebugEnabled => logger.debug(msg(verbose = true), error.e)
          case ErrorMode.LogErrors if logger.underlying.isInfoEnabled => logger.info(msg(verbose = false))
          case ErrorMode.ReturnErrors => ec.errors.add(error.copy(e = new IOException(msg(verbose = true), error.e)))
          case ErrorMode.RaiseErrors => throw new IOException(msg(verbose = true), error.e)
          case _ => // no-op
        }
        CloseableIterator.empty
    }
  }

  private def errorMessage(e: EvaluationError, input: Array[AnyRef], verbose: Boolean): String = {
    val field = if (e.field == null) { "SimpleFeature" } else { s"field '${e.field}'" }
    val terse = s"Failed to evaluate $field on line ${e.line}"
    if (!verbose) { terse } else {
      // head is the whole record
      s"$terse using values:\n${input.headOption.orNull}\n[${input.drop(1).mkString(", ")}]"
    }
  }

  override def close(): Unit = {
    CloseWithLogging(caches.values)
    CloseWithLogging(metrics)
    CloseWithLogging(validators)
  }
}

object AbstractConverter {

  type Dag = scala.collection.mutable.Map[Field, Set[Field]]

  private val IdFieldName         = "__AbstractConverter_id_field"
  private val UserDataFieldPrefix = "__AbstractConverter_user_data_"

  /**
    * Basic field implementation, useful if a converter doesn't have custom fields
    *
    * @param name field name
    * @param transforms transforms
    */
  case class BasicField(name: String, transforms: Option[Expression]) extends Field {
    override val fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  /**
    * Basic converter config implementation, useful if a converter doesn't have additional configuration
    *
    * @param `type` converter type
    * @param idField id expression
    * @param caches caches
    * @param userData user data expressions
    */
  case class BasicConfig(
      `type`: String,
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig

  /**
    * Basic converter options implementation, useful if a converter doesn't have additional options
    *
    * @param validators validator
    * @param reporters metric reporters
    * @param parseMode parse mode
    * @param errorMode error mode
    * @param encoding file/stream encoding
    */
  case class BasicOptions(
      validators: Seq[String],
      reporters: Seq[Config],
      parseMode: ParseMode,
      errorMode: ErrorMode,
      encoding: Charset
    ) extends ConverterOptions

  object BasicOptions {
    // keep as a function to pick up system property changes
    def default: BasicOptions = {
      val validators = SimpleFeatureValidator.default
      BasicOptions(validators, Seq.empty, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)
    }
  }

  /**
   * An exception used for control flow, to catch old implementations of the API with don't implement all methods.
   * Stack traces and suppressed exceptions are disabled for performance.
   *
   * @param message message
   */
  class AbstractApiError(message: String) extends Exception(message, null, false, false)

  object FieldApiError extends AbstractApiError("Field")
  object TransformerFunctionApiError extends AbstractApiError("TransformerFunction")

  /**
    * Add the dependencies of a field to a graph
    *
    * @param field field to add
    * @param fieldMap field lookup map
    * @param dag graph
    */
  private def addDependencies[F <: Field](field: Field, fieldMap: Map[String, F], dag: Dag): Unit = {
    if (!dag.contains(field)) {
      val deps = field.transforms.toSeq.flatMap(_.dependencies(Set(field), fieldMap)).toSet
      dag.put(field, deps)
      deps.foreach(addDependencies(_, fieldMap, dag))
    }
  }

  /**
    * Returns vertices in topological order.
    *
    * Note: will cause an infinite loop if there are circular dependencies
    *
    * @param dag graph
    * @return ordered vertices
    */
  private def topologicalOrder(dag: Dag): Array[Field] = {
    val res = ArrayBuffer.empty[Field]
    val remaining = new scala.collection.mutable.Queue[Field]() ++ dag.keys
    while (remaining.nonEmpty) {
      val next = remaining.dequeue()
      if (dag(next).forall(res.contains)) {
        res.append(next)
      } else {
        remaining.enqueue(next) // put at the back of the queue
      }
    }
    res.toArray
  }
}
