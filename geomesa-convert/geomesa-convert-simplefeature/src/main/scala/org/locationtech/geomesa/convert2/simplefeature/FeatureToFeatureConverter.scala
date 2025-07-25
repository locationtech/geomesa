/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.simplefeature

import com.typesafe.config.Config
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.simplefeature.FeatureToFeatureConverterFactory.FeatureToFeatureConfig
import org.locationtech.geomesa.convert2.{AbstractConverter, SimpleFeatureConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.io.InputStream

/**
  * Converter used to transform one simple feature type into another. This converter is a special use-case,
  * and does not implement the normal `process` method for parsing an input stream. Instead, call
  * `convert` directly.
  *
  * @param sft simple feature type
  * @param config converter config
  * @param fields converter fields
  * @param options converter options
  */
class FeatureToFeatureConverter(
    sft: SimpleFeatureType,
    config: FeatureToFeatureConfig,
    fields: Seq[BasicField],
    options: BasicOptions
  ) extends AbstractConverter[SimpleFeature, FeatureToFeatureConfig, BasicField, BasicOptions](
    sft, config, fields, options)  {

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] =
    throw new UnsupportedOperationException()

  override protected def values(parsed: CloseableIterator[SimpleFeature],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    var array = Array.empty[Any]
    parsed.map { feature =>
      ec.line += 1
      if (feature.getAttributeCount + 1 != array.length) {
        array = Array.ofDim(feature.getAttributeCount + 1)
      }
      var i = 0
      while (i < array.length - 1) {
        array(i) = feature.getAttribute(i)
        i += 1
      }
      array(array.length - 1) = feature.getID
      array
    }
  }
}

object FeatureToFeatureConverter {

  /**
    * Gets a typed feature to feature converter
    *
    * @param sft simple feature type
    * @param conf config
    * @return
    */
  def apply(sft: SimpleFeatureType, conf: Config): FeatureToFeatureConverter =
    SimpleFeatureConverter(sft, conf).asInstanceOf[FeatureToFeatureConverter]
}
