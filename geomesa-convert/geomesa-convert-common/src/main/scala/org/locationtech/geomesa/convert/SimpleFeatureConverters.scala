/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.util.ServiceLoader

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
  * Simplified API to build SimpleFeatureType converters
  */
object SimpleFeatureConverters extends LazyLogging {

  private[convert] val providers = {
    val pList = ServiceLoader.load(classOf[SimpleFeatureConverterFactory[_]]).toList
    logger.debug(s"Found ${pList.size} SPI providers for ${classOf[SimpleFeatureConverterFactory[_]].getName}" +
      s": ${pList.map(_.getClass.getName).mkString(", ")}")
    pList
  }

  def build[I](typeName: String, converterName: String): SimpleFeatureConverter[I] = {
    val sft = SimpleFeatureTypeLoader.sftForName(typeName)
      .getOrElse(throw new IllegalArgumentException(s"Unable to load SFT for typeName $typeName"))
    build[I](sft, converterName)
  }

  def build[I](sft: SimpleFeatureType, converterName: String): SimpleFeatureConverter[I] =
    ConverterConfigLoader.configForName(converterName).map(build[I](sft, _))
      .getOrElse(throw new IllegalArgumentException(s"Unable to find converter config for converterName $converterName"))

  def build[I](sft: SimpleFeatureType, converterConf: Config): SimpleFeatureConverter[I] = {
    providers
      .find(_.canProcess(converterConf))
      .map(_.buildConverter(sft, converterConf).asInstanceOf[SimpleFeatureConverter[I]])
      .getOrElse(throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}"))
  }
}
