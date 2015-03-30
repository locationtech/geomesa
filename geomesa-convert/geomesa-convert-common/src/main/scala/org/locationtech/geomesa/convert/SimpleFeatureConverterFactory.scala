/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert

import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr, FieldLookup, FunctionExpr}
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

trait Field {
  def name: String
  def transform: Transformers.Expr
  def eval(args: Any*)(implicit ec: EvaluationContext): Any = transform.eval(args: _*)
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

  def build[I](sft: SimpleFeatureType, conf: Config) = {
    val converterConfig = conf.getConfig("converter")
    providers
      .find(_.canProcess(converterConfig))
      .map(_.buildConverter(sft, converterConfig).asInstanceOf[SimpleFeatureConverter[I]])
      .getOrElse(throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}"))
  }
}

trait SimpleFeatureConverter[I] {
  def targetSFT: SimpleFeatureType
  def processInput(is: Iterator[I], globalParams: Map[String, String] = null): Iterator[SimpleFeature]
  def processSingleInput(i: I, globalParams: Map[String, String] = null): Option[SimpleFeature]
  def close(): Unit = {}
}

trait ToSimpleFeatureConverter[I] extends SimpleFeatureConverter[I] with Logging {
  def logErrors: Boolean = false
  def targetSFT: SimpleFeatureType
  def inputFields: IndexedSeq[Field]
  def idBuilder: Expr
  def fromInputType(i: I): Array[Any]
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

  val inputFieldIndexes = requiredFields.map(_.name).zipWithIndex.toMap

  implicit val ctx = new EvaluationContext(inputFieldIndexes, null)

  def convert(t: Array[Any], reuse: Array[Any]): SimpleFeature = {
    import spire.syntax.cfor._
    ctx.incCount()

    val attributes =
      if(reuse == null) Array.ofDim[Any](requiredFields.length)
      else reuse
    ctx.computedFields = attributes

    cfor(0)(_ < nfields, _ + 1) { i =>
      attributes(i) = requiredFields(i).eval(t: _*)
    }

    val id = idBuilder.eval(t: _*).asInstanceOf[String]
    val sf = new AvroSimpleFeature(new FeatureIdImpl(id), targetSFT)
    indexes.foreach(i => sf.setAttributeNoConvert(i._1, attributes(i._2).asInstanceOf[Object]))
    sf
  }

  val reuse = Array.ofDim[Any](requiredFields.length)

  def processSingleInput(i: I, gParams: Map[String, String] = null): Option[SimpleFeature] = {
    if (gParams != null) ctx.globalParams = Some(gParams)
    Try { convert(fromInputType(i), reuse) } match {
      case Success(s) => Some(s)
      case Failure(t) =>
        logger.debug("Failed to parse input", t)
        None
    }
  }

  def processInput(is: Iterator[I], gParams: Map[String, String] = null): Iterator[SimpleFeature] = {
    if (gParams != null) ctx.globalParams = Some(gParams)
    is.flatMap { s => processSingleInput(s) }
  }

  def resetCounter(): Unit = ctx.resetCount()

}
