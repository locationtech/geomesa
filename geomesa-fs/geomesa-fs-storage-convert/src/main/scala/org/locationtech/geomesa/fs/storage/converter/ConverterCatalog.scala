/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.converter.pathfilter.{PathFiltering, PathFilteringFactory}
import org.locationtech.geomesa.fs.storage.converter.schemes.{NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}

import java.net.URI

/**
 * Storage catalog for synthetic "converter" storage
 *
 * @param conf file system config
 */
class ConverterCatalog(val conf: Map[String, String]) extends StorageCatalog with LazyLogging {

  import ConverterCatalog._

  private val sft = {
    val sftArg =
      conf.get(SftConfigParam)
        .orElse(conf.get(SftNameParam))
        .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
    SftArgResolver.getArg(SftArgs(sftArg, null)) match {
      case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
      case Right(schema) => schema
    }
  }

  private val schemes = {
    val partitionSchemeName =
      conf.getOrElse(PartitionSchemeParam, throw new IllegalArgumentException("Must provide partition scheme name"))
    val partitionSchemeOpts =
        conf.collect { case (k, v) if k.startsWith(PartitionOptsPrefix) => k.substring(PartitionOptsPrefix.length) -> v }
    PartitionSchemeFactory.load(sft, NamedOptions(partitionSchemeName, partitionSchemeOpts))
  }

  private val converterPath = {
    val path = conf.get(ConverterPathParam).map(URI.create).getOrElse(throw new IllegalArgumentException("Must provide converter path"))
    if (path.toString.endsWith("/")) { path } else { new URI(path.toString + "/") }
  }

  private val leafStorage =
    conf.get(LeafStorageParam)
      .map(_.toBoolean)
      .getOrElse {
        val deprecated = s"${PartitionOptsPrefix}leaf-storage"
        conf.get(deprecated).fold(true) { s =>
          logger.warn(s"Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using '$LeafStorageParam'")
          s.toBoolean
        }
      }

  private val pathFiltering = conf.get(PathFilterName).flatMap { name =>
    val pathFilteringOpts =
      conf.collect { case (k, v) if k.startsWith(PathFilterOptsPrefix) => k.substring(PathFilterOptsPrefix.length) -> v }
    val factory = PathFilteringFactory.load(NamedOptions(name, pathFilteringOpts))
    if (factory.isEmpty) {
      throw new IllegalArgumentException(s"Failed to load ${classOf[PathFiltering].getName} for config '$name'")
    }
    factory
  }

  override def getTypeNames: Seq[String] = Seq(sft.getTypeName)

  override def load(typeName: String): FileSystemStorage = {
    if (typeName == sft.getTypeName) {
      val convertArg = conf.get(ConverterConfigParam).orElse(conf.get(ConverterNameParam)).getOrElse {
        throw new IllegalArgumentException(s"Must provide either converter config or name")
      }
      val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
        case Left(e) => throw new IllegalArgumentException("Could not load Converter with provided parameters", e)
        case Right(c) => c
      }
      val converter = SimpleFeatureConverter(sft, converterConfig)
      val catalog = new InMemoryCatalog()
      catalog.initialize("geomesa", java.util.Map.of())
      val ns = Namespace.of("geomesa")
      catalog.createNamespace(ns)
      val table = catalog.createTable(TableIdentifier.of(ns, "converter"), SimpleFeatureIcebergSchema.create(sft, GeoParquetWkb))
      table.updateProperties().set("geomesa.sft.name", sft.getTypeName).commit()
      val schema = SimpleFeatureIcebergSchema(table, conf.get(StorageCatalog.NamespaceConfigKey))
      new ConverterStorage(table, schema, schemes, conf, converterPath, converter, pathFiltering, leafStorage)
    } else {
      throw new IllegalArgumentException(s"Schema '$typeName' doesn't exist - available schemas: ${sft.getTypeName}")
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): FileSystemStorage =
    throw new UnsupportedOperationException("Converter storage is read-only")

  override def close(): Unit = {}
}

object ConverterCatalog {

  val CatalogType = "converter"

  val SftNameParam         = "fs.options.sft.name"
  val SftConfigParam       = "fs.options.sft.conf"
  val ConverterNameParam   = "fs.options.converter.name"
  val ConverterConfigParam = "fs.options.converter.conf"
  val ConverterPathParam   = "fs.options.converter.path"
  val PartitionSchemeParam = "fs.partition-scheme.name"
  val PartitionOptsPrefix  = "fs.partition-scheme.opts."
  val LeafStorageParam     = "fs.options.leaf-storage"
  val PathFilterName       = "fs.path-filter.name"
  val PathFilterOptsPrefix = "fs.path-filter.opts."
}
