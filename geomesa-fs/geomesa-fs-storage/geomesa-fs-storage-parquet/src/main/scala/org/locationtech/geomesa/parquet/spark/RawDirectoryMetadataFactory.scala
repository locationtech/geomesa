/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataJson
import org.locationtech.geomesa.parquet.io.SimpleFeatureParquetSchema
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer

class RawDirectoryMetadataFactory extends StorageMetadataFactory with LazyLogging {

  override def name: String = RawDirectoryMetadata.MetadataType

  /**
    * Loads a metadata instance from an existing root. The metadata info is persisted in a `metadata.json`
    * file under the root path.
    *
    * Will return a cached instance, if available. If a previous check was made to load a file from this root,
    * and the file did not exist, will not re-attempt to load it until after a configurable timeout
    *
    * @see `org.locationtech.geomesa.fs.storage.common.utils.PathCache#CacheDurationProperty()`
    * @param context file context
    * @return
    **/
  override def load(context: FileSystemContext): Option[RawDirectoryMetadata] = {
    // TODO: check for files in a better way
    // if there's a real metadata, defer to that
    if (MetadataJson.readMetadata(context).isDefined) { None } else {
      val fc = context.fc
      val files = scala.collection.mutable.Map.empty[String, ArrayBuffer[String]]
      val iter = fc.listStatus(context.root)
      while (iter.hasNext) {
        val status = iter.next()
        if (status.isFile) {
          val name = status.getPath.getName
          files.getOrElseUpdate(FilenameUtils.getExtension(name), ArrayBuffer.empty) += name
        }
      }
      logger.debug(s"Files: ${files.mapValues(_.length).mkString(", ")}")

      files.get("parquet").map { f =>
        val filter = ParquetMetadataConverter.NO_FILTER
        val footer = ParquetFileReader.readFooter(context.conf, new Path(context.root, f.head), filter)
        val sft = RawDirectoryMetadataFactory.getFeatureType(context.root.getName, footer.getFileMetaData)
        new RawDirectoryMetadata(sft, f)
      }
    }
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): RawDirectoryMetadata =
    throw new NotImplementedError("Raw directory is read only")
}

object RawDirectoryMetadataFactory extends LazyLogging {

  import org.json4s._
  import org.json4s.native.JsonMethods._

  import scala.collection.JavaConverters._

  def getFeatureType(name: String, metadata: FileMetaData): SimpleFeatureType = {
    // org.apache.spark.sql.parquet.row.metadata
    // {"type":"struct","fields":[{"name":"arrest","type":"string","nullable":true,"metadata":{}},{"name":"case_number","type":"integer","nullable":true,"metadata":{}},{"name":"dtg","type":"timestamp","nullable":true,"metadata":{}},{"name":"geom","type":{"type":"udt","class":"org.apache.spark.sql.jts.PointUDT","pyClass":"geomesa_pyspark.types.PointUDT","sqlType":{"type":"struct","fields":[{"name":"x","type":"double","nullable":true,"metadata":{}},{"name":"y","type":"double","nullable":true,"metadata":{}}]}},"nullable":true,"metadata":{}},{"name":"__fid__","type":"string","nullable":false,"metadata":{}}]}
    val spark = metadata.getKeyValueMetaData.get("org.apache.spark.sql.parquet.row.metadata") match {
      case null => Map.empty[String, String]
      case json =>
        val meta = parse(json).toOption.collect { case j: JObject => j.obj.collectFirst { case ("fields", a: JArray) => a } }.flatten.getOrElse(JArray(Nil))
        val types = meta.arr.collect { case j: JObject => j.values.get("name") -> j.values.get("type") }
        types.toMap.flatMap { case (k, v) => for (n <- k; t <- v.collect { case s: String => s }) yield n -> t }
    }
    val builder = SchemaBuilder.builder()
    metadata.getSchema.getFields.asScala.foreach { field =>
      val name = field.getName
      if (name != "__fid__") {
        if (field.isPrimitive) {
          def isString = field.getOriginalType == OriginalType.UTF8
          def isDate = spark.get(name).contains("timestamp")

          val f = field.asPrimitiveType()
          f.getPrimitiveTypeName match {
            case PrimitiveTypeName.BINARY if isString   => builder.addString(name)
            case PrimitiveTypeName.INT96 if isDate      => builder.addDate(name)
            case PrimitiveTypeName.INT32                => builder.addInt(name)
            case PrimitiveTypeName.INT64                => builder.addLong(name)
            case PrimitiveTypeName.DOUBLE               => builder.addDouble(name)
            case PrimitiveTypeName.FLOAT                => builder.addFloat(name)
            case PrimitiveTypeName.BOOLEAN              => builder.addBoolean(name)
            case PrimitiveTypeName.BINARY               => builder.addBytes(name)
            case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => builder.addBytes(name)
            case _ => logger.debug(s"skipping field $field")
          }
        } else {
          val f = field.asGroupType()
          // TODO other geom types
          if (f.getFieldCount == 2 && f.getFieldName(0) == "x" && f.getFieldName(1) == "y") {
            builder.addPoint(name)
          }
        }
      }
    }

    builder.userData(SimpleFeatureParquetSchema.EncodeFieldNames, "false").build(name)
  }
}
