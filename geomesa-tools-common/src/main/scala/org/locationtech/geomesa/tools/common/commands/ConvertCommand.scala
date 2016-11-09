/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

import java.io._
import java.util.Locale
import java.util.zip.Deflater

import com.beust.jcommander.{JCommander, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.tools.common._
import org.locationtech.geomesa.tools.common.commands.ConvertParameters._
import org.locationtech.geomesa.utils.file.FileUtils.Formats
import org.locationtech.geomesa.utils.file.FileUtils.Formats._
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.Try
class ConvertCommand(parent: JCommander) extends Command(parent) with ExportCommandTools with LazyLogging {

  override val command = "convert"
  override val params = new ConvertParameters

  override def execute() = {
    val inFMTParam = Option(params.format).flatMap(f => Try(Formats.withName(f.toLowerCase(Locale.US))).toOption)
    lazy val inFMTFile = params.files.flatMap(f => Try(Formats.withName(getFileExtension(f))).toOption).headOption
    val inFMT = inFMTParam.orElse(inFMTFile).getOrElse(Other)

    val sft = CLArgResolver.getSft(params.spec)
    logger.info(s"Using SFT definition: ${sft}")

    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    val converter = SimpleFeatureConverters.build(sft, converterConfig)

    val outFMT = Formats.withName(params.outputFormat.toLowerCase(Locale.US))
    if (outFMT == BIN && Seq(params.idAttribute, params.latAttribute, params.lonAttribute, params.labelAttribute).contains(null))
      throw new ParameterException("Missing parameters for binary export. For more information use: ./geomesa convert --help")

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val outputStream: OutputStream = createOutputStream(false, params)
    lazy val writer: Writer = getWriter(params)

    val exporter: FeatureExporter = outFMT match {
      case CSV | TSV      => new DelimitedExport(writer, outFMT, !params.noHeader)
      case SHP            => Seq(ShapefileExport.modifySchema(sft))
                             new ShapefileExport(checkShpFile(params))
      case GeoJson | JSON => new GeoJsonExport(writer)
      case GML            => new GmlExport(outputStream)
      case AVRO           => new AvroExport(outputStream, sft, avroCompression)
      case BIN            => BinFileExport(outputStream, params)
      case _              => throw new UnsupportedOperationException(s"Format $outFMT can't be exported")
    }

    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)

    try {
      params.files.foreach { file =>
        val ec = converter.createEvaluationContext(Map("inputFilePath" -> file))
        val dataIter = converter.process(new FileInputStream(file), ec)
        if (params.maxFeatures != null && params.maxFeatures >= 0) {
          logger.info(s"Converting ${getPlural(params.maxFeatures.toLong, "feature")} from $file")
          for (i <- 1 to params.maxFeatures) {
            if (dataIter.hasNext) fc.add(dataIter.next())
          }
        } else dataIter.foreach(fc.add)
        logger.info(s"Converted ${getPlural(ec.counter.getLineCount, "simple feature")} with ${getPlural(ec.counter.getSuccess, "success", "successes")} and ${getPlural(ec.counter.getFailure, "failure", "failures")}")
      }
      logger.debug(s"Applying CQL filter ${filter.toString}")
      exporter.write(fc.subCollection(filter))
    } finally {
      exporter.flush()
      exporter.close()
      IOUtils.closeQuietly(converter)
    }
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal SFT converter framework")
  class ConvertParameters extends RootExportCommands
    with BaseBinaryExportParameters
    with OptionalFeatureTypeSpecParam
    with InputFileParams {}
}
