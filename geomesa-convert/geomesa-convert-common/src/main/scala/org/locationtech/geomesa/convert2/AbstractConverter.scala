/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.{IOException, InputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Date

import com.codahale.metrics.Histogram
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.{EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics.SimpleGauge
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.validators.SimpleFeatureValidator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

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

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val requiredFields: Array[Field] = AbstractConverter.requiredFields(this)

  private val requiredFieldsCount: Int = requiredFields.length

  private val requiredFieldsIndices: Array[Int] = requiredFields.map(f => sft.indexOf(f.name))

  private val metrics = ConverterMetrics(sft, options.reporters)

  private val validators = SimpleFeatureValidator(sft, options.validators, metrics)

  private val caches = config.caches.map { case (k, v) => (k, EnrichmentCache(v)) }

  private val idFieldConfig = config.idField.orNull

  private val userDataConfig = config.userData.toArray

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
    EvaluationContext(requiredFields.map(_.name), globalParams, caches, metrics)

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val hist = ec.metrics.histogram("parse.nanos")
    val converted = convert(new ErrorHandlingIterator(parse(is, ec), options.errorMode, ec.failure, hist), ec)
    options.parseMode match {
      case ParseMode.Incremental => converted
      case ParseMode.Batch => CloseableIterator(converted.to[ListBuffer].iterator, converted.close())
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
      try { convert(raw, ec, dtgMetrics) } finally {
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
      rawValues: Array[Any],
      ec: EvaluationContext,
      dtgMetrics: Option[(Int, SimpleGauge[Date], Histogram)]): CloseableIterator[SimpleFeature] = {
    val sf = new ScalaSimpleFeature(sft, "")

    def failure(field: String, e: Throwable): CloseableIterator[SimpleFeature] = {
      ec.failure.inc()
      def msg(verbose: Boolean): String = {
        val values = if (!verbose) { "" } else {
          // head is the whole record
          s" using values:\n${rawValues.headOption.orNull}\n[${rawValues.drop(1).mkString(", ")}]"
        }
        s"Failed to evaluate field '$field' on line ${ec.line}$values"
      }

      options.errorMode match {
        case ErrorMode.RaiseErrors => throw new IOException(msg(true), e)
        case ErrorMode.SkipBadRecords if logger.underlying.isDebugEnabled => logger.underlying.debug(msg(true), e)
        case ErrorMode.SkipBadRecords if logger.underlying.isInfoEnabled => logger.underlying.info(msg(false))
        case _ => // no-op
      }
      CloseableIterator.empty
    }

    var i = 0
    try {
      ec.clear()
      while (i < requiredFieldsCount) {
        val field = requiredFields(i).eval(rawValues)(ec)
        ec.set(i, field)
        val sftIndex = requiredFieldsIndices(i)
        if (sftIndex != -1) {
          sf.setAttributeNoConvert(sftIndex, field.asInstanceOf[AnyRef])
        }
        i += 1
      }
    } catch {
      case NonFatal(e) => return failure(requiredFields(i).name, e)
    }

    // if no id builder, empty feature id will be replaced with an auto-gen one
    if (idFieldConfig != null) {
      try { sf.setId(idFieldConfig.eval(rawValues)(ec).asInstanceOf[String]) } catch {
        case NonFatal(e) => return failure("feature id", e)
      }
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    }

    i = 0
    while (i < userDataConfig.length) {
      val (k, v) = userDataConfig(i)
      try { sf.getUserData.put(k, v.eval(rawValues)(ec).asInstanceOf[AnyRef]) } catch {
        case NonFatal(e) => return failure(s"user-data:$k", e)
      }
      i += 1
    }

    val error = validators.validate(sf)
    if (error == null) {
      ec.success.inc()
      dtgMetrics.foreach { case (index, dtg, latency) =>
        val date = sf.getAttribute(index).asInstanceOf[Date]
        if (date != null) {
          dtg.set(date)
          latency.update(System.currentTimeMillis() - date.getTime)
        }
      }
      CloseableIterator.single(sf)
    } else {
      ec.failure.inc()
      val msg = s"Invalid SimpleFeature on line ${ec.line}: $error"
      options.errorMode match {
        case ErrorMode.SkipBadRecords => logger.debug(msg)
        case ErrorMode.RaiseErrors => throw new IOException(msg)
      }
      CloseableIterator.empty
    }
  }

  override def close(): Unit = {
    CloseWithLogging(caches.values)
    CloseWithLogging(metrics)
    CloseWithLogging(validators)
  }
}

object AbstractConverter {

  import scala.collection.JavaConverters._

  type Dag = scala.collection.mutable.Map[Field, Set[Field]]

  /**
    * Basic field implementation, useful if a converter doesn't have custom fields
    *
    * @param name field name
    * @param transforms transforms
    */
  case class BasicField(name: String, transforms: Option[Expression]) extends Field

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
    * Determines the fields that are actually used for the conversion
    *
    * @param converter converter
    * @tparam T intermediate parsed values binding
    * @tparam C config binding
    * @tparam F field binding
    * @tparam O options binding
    * @return
    */
  private def requiredFields[T, C <: ConverterConfig, F <: Field, O <: ConverterOptions](
      converter: AbstractConverter[T, C, F, O]): Array[Field] = {

    val fieldNameMap = converter.fields.map(f => f.name -> f).toMap
    val dag = scala.collection.mutable.Map.empty[Field, Set[Field]]

    // compute only the input fields that we need to deal with to populate the simple feature
    converter.sft.getAttributeDescriptors.asScala.foreach { ad =>
      fieldNameMap.get(ad.getLocalName).foreach(addDependencies(_, fieldNameMap, dag))
    }

    // add id field and user data deps - these will be evaluated last so we only need to add their deps
    val others = converter.config.userData.values.toSeq ++ converter.config.idField.toSeq
    others.flatMap(_.dependencies(Set.empty, fieldNameMap)).foreach(addDependencies(_, fieldNameMap, dag))

    // use a topological ordering to ensure that dependencies are evaluated before the fields that require them
    val ordered = topologicalOrder(dag)

    // log warnings for missing/unused fields
    checkMissingFields(converter, ordered.map(_.name))

    ordered
  }

  /**
    * Add the dependencies of a field to a graph
    *
    * @param field field to add
    * @param fieldMap field lookup map
    * @param dag graph
    */
  private def addDependencies(field: Field, fieldMap: Map[String, Field], dag: Dag): Unit = {
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
    val remaining = dag.keys.to[scala.collection.mutable.Queue]
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

  /**
    * Checks for missing/unused fields and logs warnings
    *
    * @param converter converter
    * @param used fields used in the conversion
    * @tparam T intermediate parsed values binding
    * @tparam C config binding
    * @tparam F field binding
    * @tparam O options binding
    */
  private def checkMissingFields[T, C <: ConverterConfig, F <: Field, O <: ConverterOptions](
      converter: AbstractConverter[T, C, F, O],
      used: Seq[String]): Unit = {
    val undefined = converter.sft.getAttributeDescriptors.asScala.map(_.getLocalName).diff(used)
    if (undefined.nonEmpty) {
      converter.logger.warn(s"'${converter.sft.getTypeName}' converter did not define fields for some attributes: " +
          undefined.mkString(", "))
    }
    val unused = converter.fields.map(_.name).diff(used)
    if (unused.nonEmpty) {
      converter.logger.warn(s"'${converter.sft.getTypeName}' converter defined unused fields: " +
          unused.mkString(", "))
    }
  }
}
