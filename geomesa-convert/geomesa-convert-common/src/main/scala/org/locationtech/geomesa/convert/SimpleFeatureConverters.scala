/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.convert

import javax.imageio.spi.ServiceRegistry

import com.typesafe.config.Config
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
  * Simplified API to build SimpleFeatureType converters
  */
object SimpleFeatureConverters {

  private[convert] val providers = ServiceRegistry.lookupProviders(classOf[SimpleFeatureConverterFactory[_]]).toList

  def build[I](typeName: String, converterName: String): SimpleFeatureConverter[I] = {
    val sft = SimpleFeatureTypeLoader.sftForName(typeName)
      .getOrElse(throw new IllegalArgumentException(s"Unable to load SFT for typeName $typeName"))
    build[I](sft, converterName)
  }

  def build[I](sft: SimpleFeatureType, converterName :String): SimpleFeatureConverter[I] =
    ConverterConfigLoader.configForName(converterName).map(build[I](sft, _))
      .getOrElse(throw new IllegalArgumentException(s"Unable to find converter config for converterName $converterName"))

  def build[I](sft: SimpleFeatureType, converterConf: Config): SimpleFeatureConverter[I] = {
    val rebased = ConverterConfigLoader.rebaseConfig(converterConf)
    providers
      .find(_.canProcess(rebased))
      .map(_.buildConverter(sft, rebased).asInstanceOf[SimpleFeatureConverter[I]])
      .getOrElse(throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}"))
  }
}
