/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.apache.iceberg.{CatalogUtil, PartitionSpec}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Metadata, StorageCatalog}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.util.Locale

class IcebergCatalog(val context: FileSystemContext) extends StorageCatalog {

  import IcebergCatalog.{RichCatalog, RichConf}

  import scala.collection.JavaConverters._

  private val namespace = Namespace.of(context.conf.required("iceberg.namespace"))
  private val catalog = IcebergCatalog.createCatalog(context)

  override def getTypeNames: Seq[String] = {
    if (catalog.namespaceExists(namespace)) {
      catalog.listTables(namespace).asScala.toSeq.flatMap { id =>
        val table = catalog.loadTable(id)
        try {
          if (table.properties().containsKey("geomesa.sft.name")) { Some(id.name) } else { None }
        } finally {
          CloseWithLogging(Option(table).collect { case c: Closeable => c })
        }
      }
    } else {
      Seq.empty
    }
  }

  override def load(typeName: String): FileSystemStorage = {
    val table = catalog.loadTable(tableId(typeName))
    val sft = SimpleFeatureTypes.createType(table.properties().get("geomesa.sft.name"), table.properties().get("geomesa.sft.spec"))
    // TODO get this from the table itself to allow for scheme migration
    val schemes = table.properties().get("geomesa.partition.spec").split(",").map(PartitionSchemeFactory.load(sft, _))
    val schema = SimpleFeatureParquetSchema(sft, context.conf)
    FileSystemStorage(context, table, schemes, schema)
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long] = None): FileSystemStorage = {
    val schema = SimpleFeatureParquetSchema(sft, context.conf)
    if (schema.geometries != GeoParquetWkb) {
      // TODO supports native geometry encoding
      throw new UnsupportedOperationException(s"Only WKB geometry encoding is supported: ${schema.geometries}")
    }
    // load the partition scheme first in case it fails
    val schemes = partitions.map(PartitionSchemeFactory.load(sft, _)).sortBy(_.name)
    val tableProps = Map(
      "geomesa.sft.name" -> sft.getTypeName,
      "geomesa.sft.spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      "geomesa.partition.spec" -> schemes.map(_.name).mkString(","),
      // file format v3 lets us use native geometries - but it's not yet supported in spark or trino
      // TableProperties.FORMAT_VERSION -> "3"
    ) ++ targetFileSize.map(s => s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}" -> s.toString).toMap

    val spec = schemes.foldLeft(PartitionSpec.builderFor(schema.iceberg))((b, m) => m.spec(b)).build()
    catalog.ensureNamespace(namespace)
    val table = catalog.createTable(tableId(sft.getTypeName), schema.iceberg, spec, null, tableProps.asJava)
    FileSystemStorage(context, table, schemes, schema)
  }

  override def close(): Unit = catalog.close()

  // TODO valid identifiers vary based on the catalog... this is for glue and not comprehensive
  private def tableId(typeName: String): TableIdentifier =
    TableIdentifier.of(namespace, typeName.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_"))
}

object IcebergCatalog {

  import scala.collection.JavaConverters._

  private implicit class RichConf(val conf: Map[String, String]) extends AnyVal {
    def required(k: String): String =
      conf.getOrElse(k, throw new IllegalArgumentException(s"Iceberg catalog requires configuration `$k` to be specified"))
  }

  private implicit class RichCatalog(val catalog: Catalog) extends AnyVal {

    def ensureNamespace(namespace: Namespace): Unit = {
      sn.foreach { supportsNamespace =>
        if (!supportsNamespace.namespaceExists(namespace)) {
          supportsNamespace.createNamespace(namespace)
        }
      }
    }

    def namespaceExists(namespace: Namespace): Boolean = sn.forall(_.namespaceExists(namespace))

    def close(): Unit = CloseWithLogging(Option(catalog).collect { case c: Closeable => c })

    private def sn: Option[SupportsNamespaces] = Option(catalog).collect { case sn: SupportsNamespaces => sn }
  }

  private def createCatalog(context: FileSystemContext): Catalog = {
    // add some defaults, to reduce boilerplate
    val defaults = Map(
      "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
      "file-format" -> "PARQUET",
      "warehouse" -> context.root.resolve("metadata/").toString
    )
    // TODO Map("parquet.filter.dictionary.enabled" -> "true")
    val props = defaults ++ context.conf
    CatalogUtil.buildIcebergCatalog("geomesa", props.asJava, null)
  }
}
