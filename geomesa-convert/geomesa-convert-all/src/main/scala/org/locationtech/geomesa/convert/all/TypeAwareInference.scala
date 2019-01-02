/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.all

import java.io.InputStream

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.avro.AvroConverterFactory
import org.locationtech.geomesa.convert.json.JsonConverterFactory
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.opengis.feature.simple.SimpleFeatureType

object TypeAwareInference {

  import org.locationtech.geomesa.convert2.SimpleFeatureConverter.factories

  /**
    * Infer a converter based on a data sample
    *
    * @param format data format (e.g. csv, avro, json, etc)
    * @param is input stream to convert
    * @param sft simple feature type, if known
    * @return
    */
  def infer(format: String,
            is: () => InputStream,
            sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    val opt = if (format.equalsIgnoreCase("avro")) {
      factories.collectFirst { case f: AvroConverterFactory => f.infer(is.apply, sft) }.flatten
    } else if (format.equalsIgnoreCase("json")) {
      factories.collectFirst { case f: JsonConverterFactory => f.infer(is.apply, sft) }.flatten
    } else if (format.equalsIgnoreCase("csv") || format.equalsIgnoreCase("tsv")) {
      factories.collectFirst { case f: DelimitedTextConverterFactory => f.infer(is.apply, sft) }.flatten
    } else {
      None
    }
    opt.orElse(SimpleFeatureConverter.infer(is, sft))
  }
}
