/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io._
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export._
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.PathUtils
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    profile(convertAndExport()) { (count, time) =>
      Command.user.info(s"Conversion complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(c => s" for $c features").getOrElse("")}")
    }
  }

  private def convertAndExport(): Option[Long] = {
    import ConvertCommand.{getConverter, getExporter}

    import scala.collection.JavaConversions._

    val files = if (params.files.nonEmpty) { params.files.iterator.flatMap(PathUtils.interpretPath) } else {
      StdInHandle.available().map(Iterator.single).getOrElse {
        throw new ParameterException("Missing option: <files>... is required")
      }
    }

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: ${SimpleFeatureTypes.encodeType(sft)}")

    val converter = getConverter(params, sft)
    val filter = Option(params.cqlFilter)
    filter.foreach(f => Command.user.debug(s"Applying CQL filter $f"))
    val ec = converter.createEvaluationContext(Map("inputFilePath" -> ""))
    val maxFeatures = Option(params.maxFeatures).map(_.intValue())

    def features() = ConvertCommand.convertFeatures(files, converter, ec, filter, maxFeatures)

    val exporter = getExporter(params, features())

    try {
      exporter.start(sft)
      val count = exporter.export(features())
      val records = ec.counter.getLineCount - (if (params.noHeader) { 0 } else { params.files.size })
      Command.user.info(s"Converted ${getPlural(records, "line")} "
          + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
          + s"and ${getPlural(ec.counter.getFailure, "failure")}")
      count
    } finally {
      IOUtils.closeQuietly(exporter)
      IOUtils.closeQuietly(converter)
    }
  }
}

object ConvertCommand extends LazyLogging {

  def getConverter(params: ConvertParameters, sft: SimpleFeatureType): SimpleFeatureConverter[Any] = {
    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    SimpleFeatureConverters.build(sft, converterConfig)
  }

  def getExporter(params: ConvertParameters, features: => Iterator[SimpleFeature]): FeatureExporter = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    lazy val writer: Writer = ExportCommand.getWriter(params.file, params.gzip)
    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val hints = {
      val q = new Query("")
      Option(params.hints).foreach { hints =>
        q.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
        ViewParams.setHints(q)
      }
      q.getHints
    }
    lazy val arrowDictionaries: Map[String, Array[AnyRef]] = {
      val attributes = hints.getArrowDictionaryFields
      if (attributes.isEmpty) { Map.empty } else {
        val values = attributes.map(a => a -> scala.collection.mutable.HashSet.empty[AnyRef])
        features.foreach(f => values.foreach { case (a, v) => v.add(f.getAttribute(a))})
        values.map { case (attribute, value) => attribute -> value.toArray }.toMap
      }
    }

    params.outputFormat match {
      case Csv | Tsv      => new DelimitedExporter(writer, params.outputFormat, None, !params.noHeader)
      case Shp            => new ShapefileExporter(FileExportCommand.checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml            => new GmlExporter(outputStream)
      case Avro           => new AvroExporter(outputStream, avroCompression)
      case Bin            => new BinExporter(hints, outputStream)
      case Arrow          => new ArrowExporter(hints, outputStream, arrowDictionaries)
      case _              => throw new ParameterException(s"Format ${params.outputFormat} is not supported.")
    }
  }

  def convertFeatures(files: Iterator[FileHandle],
                      converter: SimpleFeatureConverter[Any],
                      ec: EvaluationContext,
                      filter: Option[Filter],
                      maxFeatures: Option[Int]): Iterator[SimpleFeature] = {
    val all = files.flatMap { file =>
      ec.set(ec.indexOf("inputFilePath"), file.path)
      val is = PathUtils.handleCompression(file.open, file.path)
      converter.process(is, ec)
    }
    val filtered = filter.map(f => all.filter(f.evaluate)).getOrElse(all)
    val limited = maxFeatures.map(filtered.take).getOrElse(filtered)
    limited
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FilePropertyParams with InputFilesParam with OptionalTypeNameParam
    with RequiredFeatureSpecParam with RequiredConverterConfigParam
}
