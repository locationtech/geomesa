/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
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
  val providers = ServiceRegistry.lookupProviders(classOf[SimpleFeatureConverterFactory[_]]).toList

  def build[I](sft: SimpleFeatureType, conf: Config, path: Option[String] = None) = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._
    val converterConfig =
      (path.toSeq ++ Seq("converter", "input-converter"))
        .foldLeft(conf)( (c, p) => c.getConfigOpt(p).map(c.withFallback).getOrElse(c))


    providers
      .find(_.canProcess(converterConfig))
      .map(_.buildConverter(sft, converterConfig).asInstanceOf[SimpleFeatureConverter[I]])
      .getOrElse(throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}"))
  }
}

trait SimpleFeatureConverter[I] {
  def targetSFT: SimpleFeatureType
  def processInput(is: Iterator[I], globalParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): Iterator[SimpleFeature]
  def processWithCallback(gParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): (I) => Seq[SimpleFeature]
  def processSingleInput(i: I, globalParams: Map[String, Any] = Map.empty)(implicit ec: EvaluationContext): Seq[SimpleFeature]
  def close(): Unit = {}
}

trait ToSimpleFeatureConverter[I] extends SimpleFeatureConverter[I] with Logging {
  def logErrors: Boolean = false
  def targetSFT: SimpleFeatureType
  def inputFields: IndexedSeq[Field]
  def idBuilder: Expr
  def fromInputType(i: I): Seq[Array[Any]]
  val fieldNameMap = inputFields.map { f => (f.name, f) }.toMap

  def dependenciesOf(e: Expr): Seq[String] = e match {
    case FieldLookup(i)         => Seq(i) ++ dependenciesOf(fieldNameMap.get(i).map(_.transform).orNull)
    case FunctionExpr(_, args)  => args.flatMap { arg => dependenciesOf(arg) }
    case _                      => Seq()
  }

  val fields = inputFields.toIndexedSeq // load the input fields once, copy for safety
  val nfields = fields.length

  val sftIndices = fields.map(f => targetSFT.indexOf(f.name))
  val inputFieldIndexes = mutable.HashMap.empty[String, Int] ++= fields.map(_.name).zipWithIndex.toMap

  var reuse: Array[Any] = null

  def convert(t: Array[Any], reuse: Array[Any])(implicit ctx: EvaluationContext): SimpleFeature = {
    ctx.computedFields = reuse

    val sfValues = Array.ofDim[AnyRef](targetSFT.getAttributeCount)

    var i = 0
    while (i < nfields) {
      reuse(i) = fields(i).eval(t)
      val sftIndex = sftIndices(i)
      if (sftIndex != -1) {
        sfValues.update(sftIndex, reuse(i).asInstanceOf[AnyRef])
      }
      i += 1
    }

    val id = idBuilder.eval(t).asInstanceOf[String]
    new ScalaSimpleFeature(id, targetSFT, sfValues)
  }

  protected[this] def preProcess(i: I)(implicit ec: EvaluationContext): Option[I] = Some(i)

  override def processSingleInput(i: I, gParams: Map[String, Any])(implicit ec: EvaluationContext): Seq[SimpleFeature] = {
    val counter = ec.getCounter
    if (reuse == null || ec.fieldNameMap == null) {
      // initialize reuse and ec
      ec.fieldNameMap = inputFieldIndexes
      reuse = Array.ofDim[Any](nfields + gParams.size)
      gParams.zipWithIndex.foreach { case ((k, v), idx) =>
        val shiftedIdx = nfields + idx
        reuse(shiftedIdx) = v
        ec.fieldNameMap(k) = shiftedIdx
      }
    }
    try {
      val attributeArrays = fromInputType(i)
      attributeArrays.flatMap { attributes =>
        try {
          val res = convert(attributes, reuse)
          counter.incSuccess()
          Some(res)
        } catch {
          case e: Exception =>
            logger.warn("Failed to convert input", e)
            counter.incFailure()
            None
        }
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to parse input", e)
        Seq.empty
    }
  }

  def processWithCallback(gParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): (I) => Seq[SimpleFeature] = {
    implicit val ctx = new EvaluationContext(inputFieldIndexes, null, counter)
    (i: I) => {
      counter.incLineCount()
      preProcess(i).map(processSingleInput(_, gParams)).getOrElse(Seq.empty[SimpleFeature])
    }
  }

  def processInput(is: Iterator[I], gParams: Map[String, Any] = Map.empty, counter: Counter = new DefaultCounter): Iterator[SimpleFeature] =
    is.flatMap(processWithCallback(gParams, counter))

}
