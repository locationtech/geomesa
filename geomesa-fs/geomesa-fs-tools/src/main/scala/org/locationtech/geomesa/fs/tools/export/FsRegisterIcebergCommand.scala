/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.calrissian.mango.types.{LexiTypeEncoders, TypeEncoder}
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.core.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.core.schemes.AttributeScheme.{IntegralBucketing, WidthBucketing}
import org.locationtech.geomesa.fs.storage.core.schemes.{AttributeScheme, DateTimeScheme, HashScheme}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.parquet.io.s3.S3InputFile
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, RequiredPartitionParam}
import org.locationtech.geomesa.fs.tools.`export`.FsRegisterIcebergCommand.{FsRegisterIcebergParams, PartitionMapper}
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.FileReader
import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.util.{Collections, Locale, Properties}
import scala.util.control.NonFatal

class FsRegisterIcebergCommand extends FsDataStoreCommand with LazyLogging {

  import scala.collection.JavaConverters._

  override val params = new FsRegisterIcebergParams()

  override val name: String = "register-iceberg-files"

  override def execute(): Unit = {
    if (params.icebergConfigFile == null && params.icebergConfig.isEmpty) {
      throw new ParameterException("At least one of --iceberg-config or --iceberg-config-file must be specified")
    }

    val icebergProps = new Properties()
    if (params.icebergConfigFile != null) {
      WithClose(new FileReader(params.icebergConfigFile, StandardCharsets.UTF_8))(icebergProps.load)
    }
    params.icebergConfig.asScala.foreach { case (k, v) => icebergProps.put(k, v) }
    if (!icebergProps.containsKey("catalog-impl")) {
      throw new ParameterException("Iceberg properties must specify a 'catalog-impl'")
    }

    val catalog = try {
      Class.forName(icebergProps.getProperty("catalog-impl")).getConstructor().newInstance().asInstanceOf[Catalog]
    } catch {
      case e: Throwable => throw new ParameterException(s"Could not instantiate catalog class '$name':", e)
    }

    catalog.initialize("geomesa", Collections.unmodifiableMap(icebergProps.asInstanceOf[java.util.Map[String, String]]))

    withDataStore(execute(_, catalog, icebergProps.asInstanceOf[java.util.Map[String, String]]))

    //    val metadata = ds.storage(params.featureName).metadata
    //
    //    val fromFilter = Option(params.cqlFilter).toSeq.flatMap { f =>
    //      val keys = metadata.schemes.map { s =>
    //        s.getPartitionsForFilter(f).getOrElse {
    //          throw new ParameterException(s"The filter ${ECQL.toCQL(f)} does not select any partitions from the partition scheme ${s.name}")
    //        }
    //      }
    //      keys.foldLeft(Seq(Partition.None)) { case (partitions, keys) =>
    //        for { partition <- partitions; key <- keys } yield {
    //          Partition(partition.values + key)
    //        }
    //      }
    //    }
    //
    //    val partitions = if (params.partitions.isEmpty) { fromFilter} else { (params.partitions.asScala ++ fromFilter).distinct }
    //
    //    Command.user.info(s"Generating filters for ${partitions.size} partitions")
    //    if (!params.noHeader) {
    //      Command.output.info("Partition\tFilter")
    //    }

    //    partitions.toSeq.sortBy(_.toString).foreach { partition =>
    //      val filters = partition.values.flatMap(v => metadata.schemes.find(_.name == v.name).map(_.getCoveringFilter(v)))
    //      val filter = ECQL.toCQL(andFilters(filters.toSeq))
    //      Command.output.info(s"$partition\t$filter")
    //    }
  }

  private def execute(ds: FileSystemDataStore, catalog: Catalog, icebergProps: java.util.Map[String, String]): Unit = {
    val storage = try { ds.storage(params.featureName) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"No schema exists with type name ${params.featureName}", e)
    }
    if (storage.encoding != ParquetFileSystemStorage.Encoding) {
      throw new UnsupportedOperationException(s"Iceberg is only implemented for Parquet storage: found ${storage.encoding}")
    }

    params.partitions.asScala.foreach { p =>
      storage.metadata.schemes.foreach { s =>
        if (!p.values.exists(_.name == s.name)) {
          throw new IllegalArgumentException(
            s"Specified partition '$p' does not include all required scheme(s): ${storage.metadata.schemes.map(_.name).toSeq.sorted.mkString(", ")}")
        }
      }
    }

    val schema = SimpleFeatureParquetSchema(storage.metadata.sft, storage.context.conf).iceberg
    Command.user.info("iceberg schema: " + SchemaParser.toJson(schema))
    val partitionMappers = storage.metadata.sft.getAttributeDescriptors.asScala.flatMap { d =>
      val name = d.getLocalName
      storage.metadata.schemes.find(_.attribute == name).flatMap { scheme =>
        val mapper = PartitionMapper(scheme)
        if (mapper.isEmpty) {
          Command.user.warn(
            s"Partition scheme '${scheme.name}' is not supported by iceberg and will not be available for query filtering")
        }
        mapper
      }
    }
    val icebergPartitions = partitionMappers.foldLeft(PartitionSpec.builderFor(schema))((b, m) => m.toIceberg(b)).build()

    val namespace = Namespace.of(params.icebergNamespace)
    // TODO valid identifiers vary based on the catalog... this is for glue and not comprehensive
    val tableId = TableIdentifier.of(namespace, storage.metadata.sft.getTypeName.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_"))
    val table = if (catalog.tableExists(tableId)) {
      Command.user.info("Found existing table")
      catalog.loadTable(tableId)
      // TODO compare with existing schema and partitioning
    } else {
      Command.user.info("Creating new iceberg table")
      val warehouse = icebergProps.get("warehouse")
      if (warehouse == null) {
        throw new IllegalArgumentException("Iceberg properties must specify a 'warehouse' location")
      }
      val icebergPropsWithVersion = new java.util.HashMap[String, String]()
      icebergPropsWithVersion.putAll(icebergProps)
      // not yet supported in spark or trino
      // icebergPropsWithVersion.put(TableProperties.FORMAT_VERSION, "3")
      // force parquet to read by field ID, not position
      icebergPropsWithVersion.put(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false")
      icebergPropsWithVersion.put("read.parquet.vectorization.enabled", "false")
      // ensure field IDs are used for column resolution
      icebergPropsWithVersion.put("parquet.strict.typing", "false")
      icebergPropsWithVersion.put("parquet.avro.read-int96-as-fixed", "false")
      catalog.createTable(tableId, schema, icebergPartitions, warehouse, icebergPropsWithVersion)
    }
    Command.user.info(s"$table")

    val metricsSpec = MetricsConfig.forTable(table)

    val files = params.partitions.asScala.flatMap { p =>
      // get partition values in order (list instead of set)
      val partitionValues = partitionMappers.map { m =>
        // we should have validated that all the partitions map correctly in our setup, above
        val key = p.values.find(_.name == m.scheme.name).getOrElse {
          throw new IllegalStateException(s"Could not find associated partition: ${m.scheme.name} out of ${p.values.mkString(", ")}")
        }
        m.toIceberg(key.value)
      }
      storage.metadata.getFiles(p).map { f =>
        val url = storage.context.root.resolve(f.file)
        val s3f = new S3InputFile(storage.fs.asInstanceOf[S3ObjectStore], url)
        val footer = ParquetFileReader.readFooter(s3f, ParquetReadOptions.builder().build(), s3f.newStream())
        val pschema = ParquetMetadata.toJSON(footer)
        Command.user.info("parquet schema: " + pschema)
        logger.debug(s"Registering file $url")
        val file = table.io().newInputFile(url.toString)
        val metrics = ParquetUtil.fileMetrics(file, metricsSpec, null)
        // TODO withSort(f.sort)
        DataFiles.builder(table.spec())
          .withPath(file.location())
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(file.getLength)
          .withMetrics(metrics)
          .withPartitionValues(partitionValues.asJava)
          .withRecordCount(f.count)
          .build()
      }
    }

    // TODO filter out any files that are already registered

    Command.user.info(s"Files: ${files.mkString("\n")}")

    val append = table.newAppend()
    files.foreach(append.appendFile)
    append.commit()

    Command.user.info("Complete")
//    val manifests = files.map { file =>
//      val io = new HadoopFileIO(conf.get)
//      val ctx = TaskContext.get
//      val suffix = String.format(Locale.ROOT, "stage-%d-task-%d-manifest-%s", ctx.stageId, ctx.taskAttemptId, UUID.randomUUID)
//      val location = new Path(basePath, suffix)
//      val outputPath = FileFormat.AVRO.addExtension(location.toString)
//      val outputFile = io.newOutputFile(outputPath)
//      val writer = ManifestFiles.write(formatVersion, spec, outputFile, snapshotId)
//
//      try {
//        val writerRef = writer
//        try fileTuples.forEachRemaining((fileTuple: Tuple2[String, DataFile]) => writerRef.add(fileTuple._2)) catch {
//          case e: IOException =>
//            throw SparkExceptionUtil.toUncheckedException(e, "Unable to close the manifest writer: %s", outputPath)
//        } finally if (writerRef != null) writerRef.close()
//      }
//
//      val manifestFile = writer.toManifestFile
//    }
  }
}

object FsRegisterIcebergCommand {

  @Parameters(commandDescription = "Register GeoMesa files with an Iceberg store")
  class FsRegisterIcebergParams extends FsParams with RequiredTypeNameParam with RequiredPartitionParam {

    @Parameter(
      names = Array("--iceberg-config-file"),
      description = "Name of a configuration file, in Java properties format")
    var icebergConfigFile: String = _

    @Parameter(
      names = Array("--iceberg-config"),
      description = "Configuration properties, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var icebergConfig: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(names = Array("--iceberg-namespace"), description = "Iceberg namespace to use for tables", required = true)
    var icebergNamespace: String = _
  }

  trait PartitionMapper {
    def scheme: PartitionScheme
    def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder
    def toIceberg(key: String): String
  }

  case class HourMapper(scheme: DateTimeScheme) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.hour(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  case class DayMapper(scheme: DateTimeScheme) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.day(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }


  case class MonthMapper(scheme: DateTimeScheme) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.month(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  case class YearMapper(scheme: DateTimeScheme) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.year(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  case class HashMapper(scheme: HashScheme[_]) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.bucket(scheme.attribute, scheme.buckets)
    override def toIceberg(key: String): String = key
  }

  case class IdentityMapper(scheme: PartitionScheme, lexicoder: TypeEncoder[_, String]) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.identity(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
  }

  case class TruncateMapper(scheme: PartitionScheme, lexicoder: TypeEncoder[_, String], width: Int) extends PartitionMapper {
    override def toIceberg(b: PartitionSpec.Builder): PartitionSpec.Builder = b.truncate(scheme.attribute, width)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
  }

  object PartitionMapper {
    def apply(scheme: PartitionScheme): Option[PartitionMapper] = scheme match {
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.HOURS => Some(HourMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.DAYS => Some(DayMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.MONTHS => Some(MonthMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.YEARS => Some(YearMapper(s))
      case s: HashScheme[_] => Some(HashMapper(s))

      case s: AttributeScheme[String] =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.stringEncoder()))
          case Some(w: WidthBucketing) => Some(TruncateMapper(s, LexiTypeEncoders.stringEncoder(), w.max))
        }

      case s: AttributeScheme[Int] =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.integerEncoder()))
          case Some(i: IntegralBucketing[Int]) => Some(TruncateMapper(s, LexiTypeEncoders.integerEncoder(), i.divisor))
        }

      case s: AttributeScheme[Long] =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.longEncoder()))
          case Some(i: IntegralBucketing[Long]) => Some(TruncateMapper(s, LexiTypeEncoders.longEncoder(), i.divisor.toInt))
        }

      case _ => None
    }
  }
}
