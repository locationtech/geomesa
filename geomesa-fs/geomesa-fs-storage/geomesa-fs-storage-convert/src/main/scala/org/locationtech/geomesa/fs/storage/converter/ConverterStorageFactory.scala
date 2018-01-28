/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver, SimpleFeatureConverters}
import org.locationtech.geomesa.fs.storage.common.{FileSystemStorageFactory, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.{GeoMesaParam, SftArgResolver, SftArgs}

class ConverterStorageFactory extends FileSystemStorageFactory[ConverterStorage] {

  override val encoding: String = "converter"

  override protected def build(path: Path,
                               conf: Configuration,
                               params: java.util.Map[String, java.io.Serializable]): ConverterStorage = {
    import ConverterStorageFactory._

    val sft = {
      val sftArg = SftConfigParam.lookupOpt(params)
          .orElse(SftNameParam.lookupOpt(params))
          .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
      SftArgResolver.getArg(SftArgs(sftArg, null)) match {
        case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
        case Right(sftype) => sftype
      }
    }

    val converter = {
      val convertArg = ConverterConfigParam.lookupOpt(params)
          .orElse(ConverterNameParam.lookupOpt(params))
          .getOrElse(throw new IllegalArgumentException(s"Must provide either converter config or name"))
      val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
        case Left(e) => throw new IllegalArgumentException("Could not load Converter with provided parameters", e)
        case Right(c) => c
      }
      SimpleFeatureConverters.build(sft, converterConfig)
    }

    val partitionScheme = PartitionScheme(sft, params)

    new ConverterStorage(path, path.getFileSystem(conf), partitionScheme, sft, converter)
  }
}

object ConverterStorageFactory {
  val ConverterNameParam   = new GeoMesaParam[String]("fs.options.converter.name", "Converter Name")
  val ConverterConfigParam = new GeoMesaParam[String]("fs.options.converter.conf", "Converter Typesafe Config", largeText = true)
  val SftNameParam         = new GeoMesaParam[String]("fs.options.sft.name", "SimpleFeatureType Name")
  val SftConfigParam       = new GeoMesaParam[String]("fs.options.sft.conf", "SimpleFeatureType Typesafe Config", largeText = true)
}
