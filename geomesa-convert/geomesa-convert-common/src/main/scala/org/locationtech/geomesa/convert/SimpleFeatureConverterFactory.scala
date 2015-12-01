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
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.Transformers._
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
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

  // compute only the input fields that we need to deal with to populate the
  // simple feature
  val attrRequiredFieldsNames =
    targetSFT.getAttributeDescriptors.flatMap { ad =>
      val name = ad.getLocalName
      fieldNameMap.get(name).fold(Seq.empty[String]) { field =>
        Seq(name) ++ dependenciesOf(field.transform)
      }
    }.toSet

  val idDependencies = dependenciesOf(idBuilder)
  val requiredFieldsNames: Set[String] = attrRequiredFieldsNames ++ idDependencies
  val requiredFields = inputFields.filter { f => requiredFieldsNames.contains(f.name) }
  val nfields = requiredFields.length

  val indexes =
    targetSFT.getAttributeDescriptors.flatMap { attr =>
      val targetIdx = targetSFT.indexOf(attr.getName)
      val attrName  = attr.getLocalName
      val inputIdx  = requiredFields.indexWhere { f => f.name.equals(attrName) }

      if(inputIdx == -1) None
      else               Some((targetIdx, inputIdx))

    }.toIndexedSeq

  val inputFieldIndexes =
    mutable.HashMap.empty[String, Int] ++= requiredFields.map(_.name).zipWithIndex.toMap

  var reuse: Array[Any] = null
  def convert(t: Array[Any], reuse: Array[Any])(implicit ctx: EvaluationContext): SimpleFeature = {
    import spire.syntax.cfor._
    ctx.computedFields = reuse

    cfor(0)(_ < nfields, _ + 1) { i =>
      reuse(i) = requiredFields(i).eval(t)
    }

    val id = idBuilder.eval(t).asInstanceOf[String]
    val sf = new AvroSimpleFeature(new FeatureIdImpl(id), targetSFT)
    for((sftIdx, arrIdx) <- indexes) {
      sf.setAttributeNoConvert(sftIdx, reuse(arrIdx).asInstanceOf[Object])
    }
    sf
  }

  protected[this] def preProcess(i: I)(implicit ec: EvaluationContext): Option[I] = Some(i)

  override def processSingleInput(i: I, gParams: Map[String, Any])(implicit ec: EvaluationContext): Seq[SimpleFeature] = {
    val counter = ec.getCounter
    if(reuse == null || ec.fieldNameMap == null) {
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
