/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.converter.ConverterStorageFactory._
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}

class ConverterMetadataFactory extends StorageMetadataFactory with LazyLogging {

  import scala.collection.JavaConverters._

  override def name: String = ConverterStorage.Encoding

  override def load(context: FileSystemContext): Option[StorageMetadata] = {
    if (!Option(context.conf.get(ConverterPathParam)).contains(context.root.getName)) { None } else {
      try {
        val sft = {
          val sftArg = Option(context.conf.get(SftConfigParam))
              .orElse(Option(context.conf.get(SftNameParam)))
              .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
          SftArgResolver.getArg(SftArgs(sftArg, null)) match {
            case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
            case Right(schema) => schema
          }
        }

        val partitionSchemeOpts =
          context.conf.getValByRegex(Pattern.quote(PartitionOptsPrefix) + ".*").asScala.map {
            case (k, v) => k.substring(PartitionOptsPrefix.length) -> v
          }

        val scheme = {
          val partitionSchemeName =
            Option(context.conf.get(PartitionSchemeParam))
                .getOrElse(throw new IllegalArgumentException(s"Must provide partition scheme name"))
          PartitionSchemeFactory.load(sft, NamedOptions(partitionSchemeName, partitionSchemeOpts.toMap))
        }

        val leafStorage = Option(context.conf.get(LeafStorageParam)).map(_.toBoolean).getOrElse {
          val deprecated = partitionSchemeOpts.get("leaf-storage").map { s =>
            logger.warn("Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using " +
                s"'$LeafStorageParam'")
            s.toBoolean
          }
          deprecated.getOrElse(true)
        }

        Some(new ConverterMetadata(context, sft, scheme, leafStorage))
      } catch {
        case e: IllegalArgumentException => logger.warn(s"Couldn't create converter storage metadata: $e", e); None
      }
    }
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): StorageMetadata =
    throw new NotImplementedError("Converter storage is read only")
}
