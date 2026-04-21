/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.converter.ConverterStorageFactory._
import org.locationtech.geomesa.fs.storage.converter.pathfilter.{NamedOptions, PathFiltering, PathFilteringFactory}
import org.locationtech.geomesa.fs.storage.core.metadata.ConverterMetadata.ConverterPathParam
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, FileSystemStorageFactory, StorageMetadata}

class ConverterStorageFactory extends FileSystemStorageFactory with LazyLogging {

  override val encoding: String = ConverterStorageFactory.Encoding

  override def apply(context: FileSystemContext, metadata: StorageMetadata): FileSystemStorage = {
    val converter = {
      val convertArg = context.conf.get(ConverterConfigParam)
          .orElse(context.conf.get(ConverterNameParam))
          .getOrElse(throw new IllegalArgumentException(s"Must provide either converter config or name"))
      val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
        case Left(e) => throw new IllegalArgumentException("Could not load Converter with provided parameters", e)
        case Right(c) => c
      }
      SimpleFeatureConverter(metadata.sft, converterConfig)
    }

    val pathFilteringOpts =
      context.conf.collect { case (k, v) if k.startsWith(PathFilterOptsPrefix) => k.substring(PathFilterOptsPrefix.length) -> v }

    val pathFiltering = context.conf.get(PathFilterName).flatMap { name =>
      val factory = PathFilteringFactory.load(NamedOptions(name, pathFilteringOpts.toMap))
      if (factory.isEmpty) {
        throw new IllegalArgumentException(s"Failed to load ${classOf[PathFiltering].getName} for config '$name'")
      }
      factory
    }

    val converterPath =
      context.conf.get(ConverterPathParam).map(p => FileSystemContext.ensureTrailingSlash(context.root.resolve(p))).getOrElse {
        throw new IllegalArgumentException("Must provide converter path")
      }

    new ConverterStorage(context.copy(root = converterPath), metadata, converter, pathFiltering)
  }
}

object ConverterStorageFactory {

  val Encoding = "converter"

  val ConverterNameParam   = "fs.options.converter.name"
  val ConverterConfigParam = "fs.options.converter.conf"
  val PathFilterName       = "fs.path-filter.name"
  val PathFilterOptsPrefix = "fs.path-filter.opts."
}
