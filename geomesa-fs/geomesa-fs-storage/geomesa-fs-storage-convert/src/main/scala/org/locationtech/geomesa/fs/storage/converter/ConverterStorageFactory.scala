/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.converter.ConverterStorageFactory.{ConverterConfigParam, ConverterNameParam}

class ConverterStorageFactory extends FileSystemStorageFactory with LazyLogging {

  override val encoding: String = "converter"

  override def apply(context: FileSystemContext, metadata: StorageMetadata): FileSystemStorage = {
    val converter = {
      val convertArg = Option(context.conf.get(ConverterConfigParam))
          .orElse(Option(context.conf.get(ConverterNameParam)))
          .getOrElse(throw new IllegalArgumentException(s"Must provide either converter config or name"))
      val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
        case Left(e) => throw new IllegalArgumentException("Could not load Converter with provided parameters", e)
        case Right(c) => c
      }
      SimpleFeatureConverter(metadata.sft, converterConfig)
    }
    new ConverterStorage(context, metadata, converter)
  }

}

object ConverterStorageFactory {
  val ConverterNameParam   = "fs.options.converter.name"
  val ConverterConfigParam = "fs.options.converter.conf"
  val ConverterPathParam   = "fs.options.converter.path"
  val SftNameParam         = "fs.options.sft.name"
  val SftConfigParam       = "fs.options.sft.conf"
  val LeafStorageParam     = "fs.options.leaf-storage"
  val PartitionSchemeParam = "fs.partition-scheme.name"
  val PartitionOptsPrefix  = "fs.partition-scheme.opts."
}
