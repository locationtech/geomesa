/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.{IOException, InputStream}
import java.nio.charset.Charset

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
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
  * @tparam C config binding
  * @tparam F field binding
  * @tparam O options binding
  */
abstract class AbstractConverter[T, C <: ConverterConfig, F <: Field, O <: ConverterOptions]
  (val sft: SimpleFeatureType, val config: C, val fields: Seq[F], val options: O)
    extends SimpleFeatureConverter with ParsingConverter[T] with LazyLogging {

  private val requiredFields: Array[Field] =
    AbstractConverter.requiredFields(sft, fields, config.userData.values.toSeq ++ config.idField.toSeq)

  private val requiredFieldsCount: Int = requiredFields.length

  private val requiredFieldsIndices: Array[Int] = requiredFields.map(f => sft.indexOf(f.name))

  private val configCaches = config.caches.map { case (k, v) => (k, EnrichmentCache(v)) }

  private val idFieldConfig = config.idField.orNull

  private val userDataConfig = config.userData.toArray

  override def targetSft: SimpleFeatureType = sft

  override def createEvaluationContext(globalParams: Map[String, Any],
                                       caches: Map[String, EnrichmentCache],
                                       counter: Counter): EvaluationContext = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val globalKeys = globalParams.keys.toSeq
    val names = requiredFields.map(_.name) ++ globalKeys
    val values = Array.ofDim[Any](names.length)
    // note, globalKeys are maintained even through EvaluationContext.clear()
    globalKeys.foreachIndex { case (k, i) => values(requiredFieldsCount + i) = globalParams(k) }
    new EvaluationContextImpl(names, values, counter, configCaches ++ caches)
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val converted = convert(new ErrorHandlingIterator(parse(is, ec), options.errorMode, ec.counter), ec)
    options.parseMode match {
      case ParseMode.Incremental => converted
      case ParseMode.Batch => CloseableIterator(converted.to[ListBuffer].iterator, converted.close())
    }
  }

  override def convert(values: CloseableIterator[T], ec: EvaluationContext): CloseableIterator[SimpleFeature] =
    this.values(values, ec).flatMap(convert(_, ec))

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
  private def convert(rawValues: Array[Any], ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val sf = new ScalaSimpleFeature(sft, "")

    def failure(field: String, e: Throwable): CloseableIterator[SimpleFeature] = {
      ec.counter.incFailure()
      def msg(verbose: Boolean): String = {
        val values = if (!verbose) { "" } else {
          // head is the whole record
          s" using values:\n${rawValues.headOption.orNull}\n[${rawValues.drop(1).mkString(", ")}]"
        }
        s"Failed to evaluate field '$field' on line ${ec.counter.getLineCount}$values"
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

    val error = options.validators.validate(sf)
    if (error == null) {
      ec.counter.incSuccess()
      CloseableIterator.single(sf)
    } else {
      ec.counter.incFailure()
      val msg = s"Invalid SimpleFeature on line ${ec.counter.getLineCount}: $error"
      options.errorMode match {
        case ErrorMode.SkipBadRecords => logger.debug(msg)
        case ErrorMode.RaiseErrors => throw new IOException(msg)
      }
      CloseableIterator.empty
    }
  }

  override def close(): Unit = configCaches.foreach(_._2.close())
}

object AbstractConverter {

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
  case class BasicConfig(`type`: String,
                         idField: Option[Expression],
                         caches: Map[String, Config],
                         userData: Map[String, Expression]) extends ConverterConfig

  /**
    * Basic converter options implementation, useful if a converter doesn't have additional options
    *
    * @param validators validator
    * @param parseMode parse mode
    * @param errorMode error mode
    * @param encoding file/stream encoding
    */
  case class BasicOptions(
      validators: SimpleFeatureValidator,
      parseMode: ParseMode,
      errorMode: ErrorMode,
      encoding: Charset
    ) extends ConverterOptions

  /**
    * Determines the fields that are actually used for the conversion
    *
    * @param sft simple feature type
    * @param fields defined fields
    * @param others other expressions (i.e. id field and user data)
    * @return
    */
  private def requiredFields(sft: SimpleFeatureType, fields: Seq[Field], others: Seq[Expression]): Array[Field] = {
    import scala.collection.JavaConverters._

    val fieldNameMap = fields.map(f => (f.name, f)).toMap
    val dag = scala.collection.mutable.Map.empty[Field, Set[Field]]

    // compute only the input fields that we need to deal with to populate the simple feature
    sft.getAttributeDescriptors.asScala.foreach { ad =>
      fieldNameMap.get(ad.getLocalName).foreach(addDependencies(_, fieldNameMap, dag))
    }

    // add id field and user data deps - these will be evaluated last so we only need to add their deps
    others.flatMap(_.dependencies(Set.empty, fieldNameMap)).foreach(addDependencies(_, fieldNameMap, dag))

    // use a topological ordering to ensure that dependencies are evaluated before the fields that require them
    topologicalOrder(dag)
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
}
