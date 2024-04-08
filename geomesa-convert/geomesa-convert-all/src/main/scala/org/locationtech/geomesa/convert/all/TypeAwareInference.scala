/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.all

import com.typesafe.config.Config
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverterFactory
import org.locationtech.geomesa.convert.json.JsonConverterFactory
import org.locationtech.geomesa.convert.parquet.ParquetConverterFactory
import org.locationtech.geomesa.convert.shp.ShapefileConverterFactory
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory
import org.locationtech.geomesa.convert.xml.XmlConverterFactory
import org.locationtech.geomesa.convert2.{SimpleFeatureConverter, SimpleFeatureConverterFactory}

import java.io.InputStream
import java.util.Locale
import scala.util.Try

object TypeAwareInference {

  private val mappings = Map[String, Class[_ <: SimpleFeatureConverterFactory]](
    "avro"    -> classOf[AvroConverterFactory],
    "json"    -> classOf[JsonConverterFactory],
    "csv"     -> classOf[DelimitedTextConverterFactory],
    "tsv"     -> classOf[DelimitedTextConverterFactory],
    "parquet" -> classOf[ParquetConverterFactory],
    "shp"     -> classOf[ShapefileConverterFactory],
    "xml"     -> classOf[XmlConverterFactory]
  )

  /**
    * Infer a converter based on a data sample
    *
    * @param format data format (e.g. csv, avro, json, etc)
    * @param is input stream to convert
    * @param sft simple feature type, if known
    * @param path file path, if known
    * @return
    */
  @deprecated("replaced with `infer(String, () => InputStream, Option[SimpleFeatureType], Map[String, Any])`")
  def infer(
      format: String,
      is: () => InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(format, is, sft, path.map(EvaluationContext.inputFileParam).getOrElse(Map.empty)).toOption


  /**
   * Infer a converter based on a data sample
   *
   * @param format data format (e.g. csv, avro, json, etc)
   * @param is input stream to convert
   * @param sft simple feature type, if known
   * @param hints implementation specific hints about the input
   * @return
   */
  def infer(
      format: String,
      is: () => InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    val priority = mappings.get(format.toLowerCase(Locale.US)).map { clas =>
      Ordering.by[SimpleFeatureConverterFactory, Boolean](f => !clas.isAssignableFrom(f.getClass)) // note: false sorts first
    }
    SimpleFeatureConverter.infer(is, sft, hints, priority)
  }
}
