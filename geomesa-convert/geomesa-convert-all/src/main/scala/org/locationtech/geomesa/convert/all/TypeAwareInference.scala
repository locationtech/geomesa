/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.all

import java.io.InputStream
import java.util.Locale

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.avro.AvroConverterFactory
import org.locationtech.geomesa.convert.json.JsonConverterFactory
import org.locationtech.geomesa.convert.parquet.ParquetConverterFactory
import org.locationtech.geomesa.convert.shp.ShapefileConverterFactory
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.opengis.feature.simple.SimpleFeatureType

object TypeAwareInference {

  import org.locationtech.geomesa.convert2.SimpleFeatureConverter.factories

  private val mappings = Map[String, Any => Boolean](
    "avro"    -> (_.isInstanceOf[AvroConverterFactory]),
    "json"    -> (_.isInstanceOf[JsonConverterFactory]),
    "csv"     -> (_.isInstanceOf[DelimitedTextConverterFactory]),
    "tsv"     -> (_.isInstanceOf[DelimitedTextConverterFactory]),
    "parquet" -> (_.isInstanceOf[ParquetConverterFactory]),
    "shp"     -> (_.isInstanceOf[ShapefileConverterFactory])
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
  def infer(
      format: String,
      is: () => InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] = {
    val opt = mappings.get(format.toLowerCase(Locale.US)).flatMap(check => factories.find(check.apply))
    opt.flatMap(_.infer(is.apply, sft, path)).orElse(SimpleFeatureConverter.infer(is, sft, path))
  }
}
