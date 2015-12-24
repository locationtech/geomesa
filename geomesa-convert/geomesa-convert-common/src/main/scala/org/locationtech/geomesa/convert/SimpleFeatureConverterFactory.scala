/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import java.io.Closeable
import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.Transformers._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

trait Field {
  def name: String
  def transform: Transformers.Expr
  def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = transform.eval(args)
}

case class SimpleField(name: String, transform: Transformers.Expr) extends Field

trait SimpleFeatureConverterFactory[I] {

  def canProcess(conf: Config): Boolean

  def canProcessType(conf: Config, name: String) = Try { conf.getString("type").equals(name) }.getOrElse(false)

  def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I]

  def buildFields(fields: Seq[Config]): IndexedSeq[Field] =
    fields.map { f =>
      val name = f.getString("name")
      val transform = Transformers.parseTransform(f.getString("transform"))
      SimpleField(name, transform)
    }.toIndexedSeq

  def buildIdBuilder(t: String) = Transformers.parseTransform(t)

}

object SimpleFeatureConverters {

  import org.locationtech.geomesa.utils.conf.ConfConversions._

  val ConfigPathProperty = "org.locationtech.geomesa.converter.config.path"

  val providers = ServiceRegistry.lookupProviders(classOf[SimpleFeatureConverterFactory[_]]).toList

  lazy val confs: List[(String, Config)] = {
    val config = ConfigFactory.load()
    val path = sys.props.getOrElse(ConfigPathProperty, "geomesa.converters")
    if (!config.hasPath(path)) {
      List.empty
    } else {
      config.getConfigList(path).map { c =>
        val name = c.getStringOpt("name").orElse(c.getStringOpt("type").map(t => s"unknown[$t]")).getOrElse("unknown")
        (name, c)
      }.toList
    }
  }

  def build[I](sft: SimpleFeatureType, conf: Config, path: Option[String] = None) = {
    val converterConfig =
      (path.toSeq ++ Seq("converter", "input-converter"))
        .foldLeft(conf)( (c, p) => c.getConfigOpt(p).map(c.withFallback).getOrElse(c))

    providers
      .find(_.canProcess(converterConfig))
      .map(_.buildConverter(sft, converterConfig).asInstanceOf[SimpleFeatureConverter[I]])
      .getOrElse(throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}"))
  }
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
  def fromInputType(i: I): Seq[Array[Any]]
  val fieldNameMap = inputFields.map { f => (f.name, f) }.toMap

  // compute only the input fields that we need to deal with to populate the simple feature
  val attrRequiredFieldsNames = targetSFT.getAttributeDescriptors.flatMap { ad =>
    val name = ad.getLocalName
    fieldNameMap.get(name).fold(Seq.empty[String]) { field =>
      Seq(name) ++ Option(field.transform).map(_.dependenciesOf(fieldNameMap)).getOrElse(Seq.empty)
    }
  }.toSet

  val idDependencies = idBuilder.dependenciesOf(fieldNameMap)
  val requiredFieldsNames: Set[String] = attrRequiredFieldsNames ++ idDependencies
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
          logger.warn(s"Failed to evaluate field '${requiredFields(i).name}' using values:\n" +
              s"${t.headOption.orNull}\n[${t.tail.mkString(", ")}]", e) // head is the whole record
          return null
      }
      val sftIndex = sftIndices(i)
      if (sftIndex != -1) {
        sfValues.update(sftIndex, ec.get(i).asInstanceOf[AnyRef])
      }
      i += 1
    }

    val id = idBuilder.eval(t)(ec).asInstanceOf[String]
    new ScalaSimpleFeature(id, targetSFT, sfValues)
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
    new EvaluationContextImpl(names.toIndexedSeq, values, counter)
  }

  override def processInput(is: Iterator[I], ec: EvaluationContext): Iterator[SimpleFeature] =
    is.flatMap(i =>  processSingleInput(i, ec))
}
