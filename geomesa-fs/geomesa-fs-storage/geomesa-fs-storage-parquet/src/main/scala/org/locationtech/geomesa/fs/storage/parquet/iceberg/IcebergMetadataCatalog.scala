/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.StorageMetadataCatalog.CatalogSpi
import org.locationtech.geomesa.fs.storage.core.metadata.namespaced
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, Metadata, PartitionSchemeFactory, StorageMetadataCatalog}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.Locale

/**
 * Catalog for iceberg-based metadata
 *
 * @param context file system
 */
class IcebergMetadataCatalog(context: FileSystemContext) extends StorageMetadataCatalog with LazyLogging {

  import IcebergMetadataCatalog.IcebergProps

  import scala.collection.JavaConverters._

  private val props = new IcebergProps(context)

  private val namespace = props.namespace
  private val catalog = props.catalog

  override def getTypeNames: Seq[String] = {
    // TODO filter based on some gm-specific keys?
    catalog.listTables(namespace).asScala.map(_.name()).toSeq
  }

  override def load(typeName: String): IcebergMetadata = {
    val table = catalog.loadTable(tableId(typeName))
    val sft = SimpleFeatureTypes.createType(table.properties().get("geomesa.sft.name"), table.properties().get("geomesa.sft.spec"))
    val schemes = table.properties().get("geomesa.partition.spec").split(",").map(PartitionSchemeFactory.load(sft, _))
    val mapper = new IcebergMapper(namespaced(sft, context.namespace), schemes, context)
    new IcebergMetadata(table, mapper)
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): IcebergMetadata = {
    // load the partition scheme first in case it fails
    val schemes = partitions.map(PartitionSchemeFactory.load(sft, _))
    val mapper = new IcebergMapper(namespaced(sft, context.namespace), schemes, context)
    val tableProps = Map(
      "geomesa.sft.name" -> sft.getTypeName,
      "geomesa.sft.spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      "geomesa.partition.spec" -> schemes.map(_.name).mkString(","),
      // file format v3 lets us use native geometries - but it's not yet supported in spark or trino
      // TableProperties.FORMAT_VERSION -> "3"
    ) ++ targetFileSize.map(s => s"${IcebergMetadata.PropertyPrefix}${Metadata.TargetFileSize}" -> s.toString).toMap

    val table = catalog.createTable(tableId(sft.getTypeName), mapper.schema, mapper.spec, null, tableProps.asJava)
    new IcebergMetadata(table, mapper)
  }

  // TODO valid identifiers vary based on the catalog... this is for glue and not comprehensive
  private def tableId(typeName: String): TableIdentifier =
    TableIdentifier.of(namespace, typeName.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_"))
}

object IcebergMetadataCatalog {

  private val KeyPrefix = "iceberg."

  private class IcebergProps(val context: FileSystemContext) extends AnyVal {

    import scala.collection.JavaConverters._

    def namespace: Namespace = Namespace.of(required(s"${KeyPrefix}namespace"))

    def catalog: Catalog = {
      val impl = required(s"${KeyPrefix}catalog-impl")
      val catalog = try { Class.forName(impl).getConstructor().newInstance().asInstanceOf[Catalog] } catch {
        case e: Throwable => throw new RuntimeException(s"Could not instantiate catalog class '$impl':", e)
      }
      // map our normal s3 config keys to iceberg ones, so they don't have to be configured 2x
      val s3Configs = context.conf.collect {
        case ("fs.s3.force-path-style", v) => "s3.path-style-access" -> v
        case ("fs.s3.region", v) => "client.region" -> v
        case (k, v) if k.startsWith("fs.s3") => k.substring(3) -> v
      }
      // add some defaults, to reduce boilerplate
      val defaults = Map("io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
        "file-format" -> "PARQUET",
        "warehouse" -> context.root.resolve("metadata/").toString
      )
      val props =
        defaults ++ s3Configs ++ context.conf.collect { case (k, v) if k.startsWith(KeyPrefix) => k.substring(KeyPrefix.length) -> v }
      catalog.initialize("geomesa", props.asJava)
      catalog
    }

    private def required(k: String): String =
      context.conf.getOrElse(k, throw new IllegalArgumentException(s"Iceberg catalog requires configuration `$k` to be specified"))
  }


  class IcebergCatalogSpi extends CatalogSpi {
    override def `type`: String = IcebergMetadata.MetadataType
    override def apply(context:  FileSystemContext): StorageMetadataCatalog = new IcebergMetadataCatalog(context)
  }
}
