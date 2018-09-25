/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.Optional
import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.{StorageMetadata, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}
import org.opengis.feature.simple.SimpleFeatureType

class ConverterStorageFactory extends FileSystemStorageFactory with LazyLogging {

  import ConverterStorageFactory._

  import scala.collection.JavaConverters._

  override def getEncoding: String = "converter"

  override def load(fc: FileContext,
                    conf: Configuration,
                    root: Path): Optional[FileSystemStorage] = {
    if (Option(conf.get(ConverterPathParam)).exists(_ != root.getName) || StorageMetadata.load(fc, root).isDefined) {
      Optional.empty()
    } else {
      try {
        val sft = {
          val sftArg = Option(conf.get(SftConfigParam))
              .orElse(Option(conf.get(SftNameParam)))
              .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
          SftArgResolver.getArg(SftArgs(sftArg, null)) match {
            case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
            case Right(schema) => schema
          }
        }

        val converter = {
          val convertArg = Option(conf.get(ConverterConfigParam))
              .orElse(Option(conf.get(ConverterNameParam)))
              .getOrElse(throw new IllegalArgumentException(s"Must provide either converter config or name"))
          val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
            case Left(e) => throw new IllegalArgumentException("Could not load Converter with provided parameters", e)
            case Right(c) => c
          }
          SimpleFeatureConverter(sft, converterConfig)
        }

        val partitionScheme = {
          val partitionSchemeName =
            Option(conf.get(PartitionSchemeParam))
                .getOrElse(throw new IllegalArgumentException(s"Must provide partition scheme name"))
          val partitionSchemeOpts =
            conf.getValByRegex(Pattern.quote(PartitionOptsPrefix) + ".*").asScala.map {
              case (k, v) => k.substring(PartitionOptsPrefix.length) -> v
            }
          PartitionScheme(sft, partitionSchemeName, partitionSchemeOpts.asJava)
        }

        Optional.of(new ConverterStorage(fc, root, sft, converter, partitionScheme))
      } catch {
        case e: IllegalArgumentException => logger.warn(s"Couldn't create converter storage: $e", e); Optional.empty()
      }
    }
  }

  override def create(fc: FileContext,
                      conf: Configuration,
                      root: Path,
                      sft: SimpleFeatureType): FileSystemStorage = {
    throw new NotImplementedError("Converter FS is read only")
  }
}

object ConverterStorageFactory {
  val ConverterNameParam   = "fs.options.converter.name"
  val ConverterConfigParam = "fs.options.converter.conf"
  val ConverterPathParam   = "fs.options.converter.path"
  val SftNameParam         = "fs.options.sft.name"
  val SftConfigParam       = "fs.options.sft.conf"
  val PartitionSchemeParam = "fs.partition-scheme.name"

  val PartitionOptsPrefix  = "fs.partition-scheme.opts."
}
