/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.{Closeable, InputStream}
import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.Transformers._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

trait Field {
  def name: String
  def transform: Transformers.Expr
  def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = transform.eval(args)
}

case class SimpleField(name: String, transform: Transformers.Expr) extends Field

object StandardOptions {
  val Validating = "options.validating"
  val LineMode   = "options.line-mode"
}

trait SimpleFeatureConverterFactory[I] {
  def canProcess(conf: Config): Boolean
  def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I]
}

abstract class AbstractSimpleFeatureConverterFactory[I] extends SimpleFeatureConverterFactory[I] {

  override def canProcess(conf: Config): Boolean =
    if (conf.hasPath("type")) conf.getString("type").equals(typeToProcess) else false

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val idBuilder = buildIdBuilder(conf)
    val fields = buildFields(conf)
    val userDataBuilder = buildUserDataBuilder(conf)
    val validating = isValidating(conf)
    buildConverter(sft, conf, idBuilder, fields, userDataBuilder, validating)
  }

  protected def typeToProcess: String

  protected def buildConverter(sft: SimpleFeatureType,
                               conf: Config,
                               idBuilder: Expr,
                               fields: IndexedSeq[Field],
                               userDataBuilder: Map[String, Expr],
                               validating: Boolean): SimpleFeatureConverter[I]

  protected def buildFields(conf: Config): IndexedSeq[Field] =
    conf.getConfigList("fields").map(buildField).toIndexedSeq

  protected def buildField(field: Config): Field =
    SimpleField(field.getString("name"), Transformers.parseTransform(field.getString("transform")))

  protected def buildIdBuilder(conf: Config): Expr = {
    if (conf.hasPath("id-field")) {
      Transformers.parseTransform(conf.getString("id-field"))
    } else {
      Transformers.parseTransform("null")
    }
  }

  protected def buildUserDataBuilder(conf: Config): Map[String, Expr] = {
    if (conf.hasPath("user-data")) {
      conf.getConfig("user-data").entrySet.map { e =>
        e.getKey -> Transformers.parseTransform(e.getValue.unwrapped().toString)
      }.toMap
    } else {
      Map.empty
    }
  }

  protected def isValidating(conf: Config): Boolean =
    if (conf.hasPath(StandardOptions.Validating)) conf.getBoolean(StandardOptions.Validating) else true
}

trait SimpleFeatureConverter[I] extends Closeable {

  /**
   * Result feature type
   */
  def targetSFT: SimpleFeatureType

  /**
   * Stream process inputs into simple features
   */
  def processInput(is: Iterator[I], ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature]

  def processSingleInput(i: I, ec: EvaluationContext = createEvaluationContext()): Seq[SimpleFeature]

  def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature]

  /**
   * Creates a context used for processing
   */
  def createEvaluationContext(globalParams: Map[String, Any] = Map.empty,
                              counter: Counter = new DefaultCounter): EvaluationContext = {
    val keys = globalParams.keys.toIndexedSeq
    val values = keys.map(globalParams.apply).toArray
    EvaluationContext(keys, values, counter)
  }

  override def close(): Unit = {}
}

/**
 * Base trait to create a simple feature converter
 */
trait ToSimpleFeatureConverter[I] extends SimpleFeatureConverter[I] with LazyLogging {

  def targetSFT: SimpleFeatureType
  def inputFields: IndexedSeq[Field]
  def idBuilder: Expr
  def userDataBuilder: Map[String, Expr]
  def fromInputType(i: I): Seq[Array[Any]]
  def validating: Boolean

  val validate: (SimpleFeature, EvaluationContext) => Boolean = {
    val valid: (SimpleFeature) => Boolean = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      val dtgFn: (SimpleFeature) => Boolean = targetSFT.getDtgIndex match {
        case Some(dtgIdx) => (sf: SimpleFeature) => sf.getAttribute(dtgIdx) != null
        case None => (_) => true
      }
      val geomFn: (SimpleFeature) => Boolean =
        if (targetSFT.getGeometryDescriptor != null) {
          (sf: SimpleFeature) => sf.getDefaultGeometry != null
        } else {
          (_) => true
        }
      (sf: SimpleFeature) => dtgFn(sf) && geomFn(sf)
      }

    if (validating) {
      (sf: SimpleFeature, ec: EvaluationContext) => {
        val v = valid(sf)
        if (!v) {
          logger.debug(s"Invalid SimpleFeature on line ${ec.counter.getLineCount}")
        }
        v
      }
    } else (sf: SimpleFeature, ec: EvaluationContext) => true
  }

  val fieldNameMap = inputFields.map(f => (f.name, f)).toMap

  // compute only the input fields that we need to deal with to populate the simple feature
  val attrRequiredFieldsNames = targetSFT.getAttributeDescriptors.flatMap { ad =>
    val name = ad.getLocalName
    fieldNameMap.get(name).fold(Seq.empty[String]) { field =>
      Seq(name) ++ Option(field.transform).map(_.dependenciesOf(fieldNameMap)).getOrElse(Seq.empty)
    }
  }.toSet

  val idDependencies = idBuilder.dependenciesOf(fieldNameMap)
  val userDataDependencies = userDataBuilder.values.flatMap(_.dependenciesOf(fieldNameMap)).toSet
  val requiredFieldsNames: Set[String] = attrRequiredFieldsNames ++ idDependencies ++ userDataDependencies
  val requiredFields = inputFields.filter(f => requiredFieldsNames.contains(f.name))
  val nfields = requiredFields.length

  val sftIndices = requiredFields.map(f => targetSFT.indexOf(f.name))
  val inputFieldIndexes = mutable.HashMap.empty[String, Int] ++= requiredFields.map(_.name).zipWithIndex.toMap

  /**
   * Convert input values into a simple feature with attributes
   */
  def convert(t: Array[Any], ec: EvaluationContext): SimpleFeature = {
    val sfValues = Array.ofDim[AnyRef](targetSFT.getAttributeCount)

    var i = 0
    while (i < nfields) {
      try {
        ec.set(i, requiredFields(i).eval(t)(ec))
      } catch {
        case e: Exception =>
          val valuesStr = if (t.length > 0) t.tail.mkString(", ") else ""
          logger.warn(s"Failed to evaluate field '${requiredFields(i).name}' using values:\n" +
            s"${t.headOption.orNull}\n[$valuesStr]", e) // head is the whole record
          return null
      }
      val sftIndex = sftIndices(i)
      if (sftIndex != -1) {
        sfValues.update(sftIndex, ec.get(i).asInstanceOf[AnyRef])
      }
      i += 1
    }

    val id = idBuilder.eval(t)(ec).asInstanceOf[String]
    val sf = new ScalaSimpleFeature(id, targetSFT, sfValues)
    userDataBuilder.foreach { case (k, v) => sf.getUserData.put(k, v.eval(t)(ec).asInstanceOf[AnyRef]) }

    if (validate(sf, ec)) sf else null
  }

  /**
   * Process a single input (e.g. line)
   */
  def processSingleInput(i: I, ec: EvaluationContext): Seq[SimpleFeature] = {
    ec.counter.incLineCount()

    val attributes = try { fromInputType(i) } catch {
      case e: Exception => logger.warn(s"Failed to parse input '$i'", e); Seq.empty
    }

    val (failures, successes) = attributes.map(convert(_, ec)).partition(_ == null)
    ec.counter.incSuccess(successes.length)
    if (failures.nonEmpty) {
      ec.counter.incFailure(failures.length)
    }
    successes
  }

  override def createEvaluationContext(globalParams: Map[String, Any], counter: Counter): EvaluationContext = {
    val globalKeys = globalParams.keys.toSeq
    val names = requiredFields.map(_.name) ++ globalKeys
    val values = Array.ofDim[Any](names.length)
    globalKeys.zipWithIndex.foreach { case (k, i) => values(requiredFields.length + i) = globalParams(k) }
    new EvaluationContextImpl(names, values, counter)
  }

  override def processInput(is: Iterator[I], ec: EvaluationContext): Iterator[SimpleFeature] =
    is.flatMap(i =>  processSingleInput(i, ec))
}

trait LinesToSimpleFeatureConverter extends ToSimpleFeatureConverter[String] {

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] =
    processInput(IOUtils.lineIterator(is, StandardCharsets.UTF_8.displayName), ec)

}
