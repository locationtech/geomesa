/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.simplefeature

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{Expr, ExpressionWrapper, FieldLookup}
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable

@deprecated("replaced with FeatureToFeatureConverter")
class SimpleFeatureSimpleFeatureConverterFactory extends AbstractSimpleFeatureConverterFactory[SimpleFeature] {

  override protected def typeToProcess: String = "simple-feature"

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[SimpleFeature] = {

    val inputSFT = SimpleFeatureTypeLoader.sftForName(conf.getString("input-sft")).getOrElse(throw new RuntimeException("Cannot find input sft"))
    // TODO: does this have any implications for global params in the evaluation context?
    val idBuilder =
      if(conf.hasPath("id-field")) buildIdBuilder(conf)
      else Transformers.Col(inputSFT.getAttributeCount) // FID is put in as the last attribute, we copy it over here

    val fields = buildFields(conf, inputSFT)

    val userDataBuilder = buildUserDataBuilder(conf)
    val cacheServices = buildCacheService(conf)
    val parseOpts = getParsingOptions(conf, sft)
    buildConverter(inputSFT, sft, conf, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)
  }

  override protected def buildConverter(sft: SimpleFeatureType, conf: Config, idBuilder: Expr, fields: immutable.IndexedSeq[Field], userDataBuilder: Map[String, Expr], cacheServices: Map[String, EnrichmentCache], parseOpts: ConvertParseOpts): SimpleFeatureConverter[SimpleFeature] = ???

  def buildConverter(inputSFT: SimpleFeatureType,
                     sft: SimpleFeatureType,
                     conf: Config,
                     idBuilder: Transformers.Expr,
                     fields: IndexedSeq[Field],
                     userDataBuilder: Map[String, Transformers.Expr],
                     cacheServices: Map[String, EnrichmentCache],
                     parseOpts: ConvertParseOpts): SimpleFeatureConverter[SimpleFeature] = {
    new SimpleFeatureSimpleFeatureConverter(inputSFT, sft, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)
  }

  def buildFields(conf: Config, inputSFT: SimpleFeatureType): IndexedSeq[Field] = {
    import scala.collection.JavaConversions._
    val fields = conf.getConfigList("fields").map(buildField(_, inputSFT)).toIndexedSeq
    val undefined = inputSFT.getAttributeDescriptors.map(_.getLocalName).toSet.diff(fields.map(_.name).toSet)
    val defaultFields = undefined.map { f => SimpleFeatureField(f, Transformers.Col(inputSFT.indexOf(f))) }.toIndexedSeq
    defaultFields ++ fields
  }

  def buildField(field: Config, inputSFT: SimpleFeatureType): Field =
    SimpleFeatureField(field.getString("name"), buildTransform(field, inputSFT))


  private def buildTransform(field: Config, inputSFT: SimpleFeatureType) = {
    if(!field.hasPath("transform")) Transformers.Col(inputSFT.indexOf(field.getString("name")))
    else Transformers.parseTransform(field.getString("transform")) match {
      // convert field lookups to col lookups
      case FieldLookup(n) => Transformers.Col(inputSFT.indexOf(n))
      case ExpressionWrapper(Expression.FieldLookup(n)) => Transformers.Col(inputSFT.indexOf(n))
      case t => t
    }
  }
}

case class SimpleFeatureField(name: String, transform: Expr) extends Field {
  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any =
    transform.eval(args)
}
