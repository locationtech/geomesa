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
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.annotations.ConverterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.SimpleFeatureType
import org.reflections.Reflections

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Simplified API to build SimpleFeatureType converters
  */
object SimpleFeatureConverters extends LazyLogging {

  private[convert] val providers = {
    val spiList = ServiceRegistry.lookupProviders(classOf[SimpleFeatureConverterFactory[_]]).toList
    logger.info(s"Found ${spiList.size} SPI providers for ${classOf[SimpleFeatureConverterFactory[_]].getName}" +
      s": ${spiList.map(_.getClass.getName).mkString(", ")}")

    val refProv = new mutable.ListBuffer[SimpleFeatureConverterFactory[_]]()
    new Reflections("org.locationtech.geomesa.convert")
      .getTypesAnnotatedWith(classOf[ConverterFactory]).foreach { case cls =>
         if (classOf[SimpleFeatureConverterFactory[_]].isAssignableFrom(cls)) {
            refProv += cls.newInstance().asInstanceOf[SimpleFeatureConverterFactory[_]]
          } else {
            logger.warn(s"Class annotated with ${classOf[ConverterFactory].getName} must " +
              s"implement ${classOf[SimpleFeatureConverterFactory[_]]} in order to load converters")
          }
        }

    logger.info(s"Registered ${refProv.size} annotated providers for ${classOf[ConverterFactory].getSimpleName}" +
      s": ${refProv.map(_.getClass.getName).mkString(", ")}")

    import scala.collection.JavaConverters._
    (spiList ++ refProv).asJava
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
