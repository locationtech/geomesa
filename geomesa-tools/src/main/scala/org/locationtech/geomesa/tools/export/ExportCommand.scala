/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.{Deflater, GZIPOutputStream}

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.formats.ExportFormats.ExportFormat
import org.locationtech.geomesa.tools.export.formats.FileSystemExporter.{OrcFileSystemExporter, ParquetFileSystemExporter}
import org.locationtech.geomesa.tools.export.formats.{BinExporter, NullExporter, ShapefileExporter, _}
import org.locationtech.geomesa.tools.utils.ParameterConverters.ExportFormatConverter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait ExportCommand[DS <: DataStore] extends DataStoreCommand[DS] with MethodProfiling {

  override val name = "export"
  override def params: ExportParams

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Feature export complete to ${Option(params.file).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(" for " + _ + " features").getOrElse("")}")

    profile(complete _)(withDataStore(export))
  }

  protected def export(ds: DS): Option[Long] = {
    import ExportCommand._
    import org.locationtech.geomesa.tools.export.formats.ExportFormats._

    val format = getOutputFormat(params)
    val query = createQuery(getSchema(ds), format, params)

    val features = try { getFeatures(ds, query) } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct", e)
    }

    // noinspection ComparingUnrelatedTypes
    lazy val fids = !Option(query.getHints.get(QueryHints.ARROW_INCLUDE_FID)).contains(java.lang.Boolean.FALSE)
    lazy val dictionaries = ArrowExporter.queryDictionaries(ds, query)
    lazy val avroCompression = if (params.gzip == null) { Deflater.DEFAULT_COMPRESSION } else {
      val compression = params.gzip.toInt
      params.gzip = null // disable compressing the output stream, as it's handled by the avro writer
      compression
    }

    val exporter = format match {
      case Arrow          => new ArrowExporter(query.getHints, createOutputStream(params), dictionaries)
      case Avro           => new AvroExporter(avroCompression, createOutputStream(params))
      case Bin            => new BinExporter(query.getHints, createOutputStream(params))
      case Csv            => DelimitedExporter.csv(createWriter(params), !params.noHeader, fids)
      case GeoJson | Json => new GeoJsonExporter(createWriter(params))
      case Gml | Xml      => new GmlExporter(createOutputStream(params))
      case Html | Leaflet => new LeafletMapExporter(params)
      case Null           => NullExporter
      case Orc            => new OrcFileSystemExporter(ensureOutputFile(params, format))
      case Parquet        => new ParquetFileSystemExporter(ensureOutputFile(params, format))
      case Shp            => new ShapefileExporter(new File(ensureOutputFile(params, format)))
      case Tsv            => DelimitedExporter.tsv(createWriter(params), !params.noHeader, fids)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _ => throw new UnsupportedOperationException(s"Format $format can't be exported")
    }

    try {
      exporter.start(features.getSchema)
      export(exporter, features)
    } finally {
      CloseWithLogging(exporter)
    }
  }

  protected def export(exporter: FeatureExporter, collection: SimpleFeatureCollection): Option[Long] =
    WithClose(CloseableIterator(collection.features()))(exporter.export)

  protected def getSchema(ds: DS): SimpleFeatureType = params match {
    case p: TypeNameParam => ds.getSchema(p.featureName)
  }

  protected def getFeatures(ds: DS, query: Query): SimpleFeatureCollection =
    ds.getFeatureSource(query.getTypeName).getFeatures(query)
}

object ExportCommand extends LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Create the query to execute
    *
    * @param toSft lazy simple feature type
    * @param fmt export format
    * @param params parameters
    * @return
    */
  def createQuery(toSft: => SimpleFeatureType, fmt: ExportFormat, params: ExportParams): Query = {
    val typeName = Option(params).collect { case p: TypeNameParam => p.featureName }.orNull
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
    lazy val sft = toSft // only evaluate it once

    val query = new Query(typeName, filter)
    Option(params.maxFeatures).map(Int.unbox).foreach(query.setMaxFeatures)
    Option(params).collect { case p: OptionalIndexParam => p }.foreach { p =>
      Option(p.index).foreach { index =>
        logger.debug(s"Using index $index")
        query.getHints.put(QueryHints.QUERY_INDEX, index)
      }
    }

    Option(params.hints).foreach { hints =>
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
      ViewParams.setHints(query)
    }

    if (fmt == ExportFormats.Arrow) {
      query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
    } else if (fmt == ExportFormats.Bin) {
      // if not specified in hints, set it here to trigger the bin query
      if (!query.getHints.containsKey(QueryHints.BIN_TRACK)) {
        query.getHints.put(QueryHints.BIN_TRACK, "id")
      }
    }

    val attributes: Array[String] = {
      val provided = Option(params.attributes).collect { case a if !a.isEmpty => a.asScala }
      if (fmt == ExportFormats.Bin) {
        provided.getOrElse(BinAggregatingScan.propertyNames(query.getHints, sft)).toArray
      } else if (fmt == ExportFormats.Shp) {
        provided.map(ShapefileExporter.replaceGeom(sft, _)).getOrElse(ShapefileExporter.modifySchema(sft)).toArray
      } else {
        provided match {
          case None => null
          case Some(p) =>
            val (id, attributes) = p.partition(_.equalsIgnoreCase("id"))
            if (id.isEmpty && !query.getHints.containsKey(QueryHints.ARROW_INCLUDE_FID)) {
              query.getHints.put(QueryHints.ARROW_INCLUDE_FID, java.lang.Boolean.FALSE)
            }
            attributes.toArray
        }
      }
    }

    query.setPropertyNames(attributes)

    logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
    logger.debug(s"Applying transform ${Option(attributes).map(_.mkString(",")).orNull}")

    QueryRunner.configureDefaultQuery(sft, query)
  }

  /**
    * Create an output stream to write to the given file
    *
    * @param params parameters
    * @return
    */
  def createOutputStream(params: ExportParams): OutputStream = {
    val out = if (params.file == null) {
      System.out
    } else if (PathUtils.isRemote(params.file)) {
      PathUtils.getUrl(params.file).openConnection().getOutputStream
    } else {
      new FileOutputStream(new File(params.file))
    }
    val compressed = if (params.gzip == null) { out } else {
      // hack to access the protected deflate level
      new GZIPOutputStream(out) { `def`.setLevel(params.gzip) }
    }
    new BufferedOutputStream(compressed)
  }

  /**
    * Create an output writer
    *
    * @param params parameters
    * @return
    */
  def createWriter(params: ExportParams): Writer = new OutputStreamWriter(createOutputStream(params))

  /**
    * Ensure that an output file is specified
    *
    * @param params parameters
    * @param format output format (used for error message)
    * @return
    */
  def ensureOutputFile(params: ExportParams, format: ExportFormat): String = {
    if (params.file != null) { params.file } else {
      throw new ParameterException(s"Error: Output format '$format' requires file-based output, please use --output")
    }
  }

  /**
    * Gets the output format to use
    *
    * @param params parameters
    * @return
    */
  def getOutputFormat(params: ExportParams): ExportFormat = {
    def direct: Option[ExportFormat] = Option(params.outputFormat)
    def file: Option[ExportFormat] = Option(params.file).flatMap { f =>
      val ext = PathUtils.getUncompressedExtension(f)
      ExportFormats.values.find(_.toString.equalsIgnoreCase(ext))
    }
    direct.orElse(file).getOrElse(ExportFormats.Csv)
  }

  /**
    * Export parameters
    */
  trait ExportParams extends OptionalCqlFilterParam with QueryHintsParams {
    @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
    var file: String = _

    @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
    var gzip: Integer = _

    @Parameter(names = Array("-F", "--output-format"), description = "File format of output files (csv|tsv|gml|json|shp|avro|leaflet|orc|parquet|arrow)", required = false, converter = classOf[ExportFormatConverter])
    var outputFormat: ExportFormat = _

    @Parameter(names = Array("--no-header"), description = "Export as a delimited text format (csv|tsv) without a type header", required = false)
    var noHeader: Boolean = false

    @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. default: Unlimited")
    var maxFeatures: Integer = _

    @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export (comma-separated)...Comma-separated expressions with each in the format attribute[=filter_function_expression]|derived-attribute=filter_function_expression|'id'. 'id' will export the feature ID, filter_function_expression is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested")
    var attributes: java.util.List[String] = _
  }
}
