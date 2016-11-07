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
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.locationtech.geomesa.utils.file.FileUtils.Formats
import org.locationtech.geomesa.utils.file.FileUtils.Formats._
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

    lazy val outputStream: OutputStream = createOutputStream(false, params)
    lazy val writer: Writer = getWriter(params)

    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else if (params.featureName != null) {
        // Guess and look for featureName-Type e.g. example-json
        val conf = params.featureName + "-" + inFMT.toString
        logger.info(s"No converter config specified. Looking for $conf")
        CLArgResolver.getConfig(conf)
      } else {
        throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
      }
    }
    val converter = SimpleFeatureConverters.build(sft, converterConfig)

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val outFMT = Formats.withName(params.outputFormat.toLowerCase(Locale.US))

    val exporter: FeatureExporter = outFMT match {
      case CSV | TSV      => new DelimitedExport(writer, outFMT, !params.noHeader)
      case SHP            =>
        if (params.attributes == null) {
          Seq(ShapefileExport.modifySchema(sft))
        } else {
          Seq(ShapefileExport.replaceGeomInAttributesString(params.attributes, sft))
        }
        new ShapefileExport(checkShpFile(params))
      case GeoJson | JSON => new GeoJsonExport(writer)
      case GML            => new GmlExport(outputStream)
      case AVRO           => new AvroExport(outputStream, sft, avroCompression)
      case BIN            => BinFileExport(outputStream, params)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format $outFMT can't be exported")
    }

    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    logger.debug(s"Applying CQL filter ${filter.toString}")
    val fc = new DefaultFeatureCollection()

    try {
      params.files.foreach { file =>
        val is: InputStream = new FileInputStream(file)
        val dataIter = converter.process(is)
        if (params.maxFeatures != null && params.maxFeatures > 0) {
          logger.info(s"Exporting ${params.maxFeatures} ${getPlural(params.maxFeatures.toLong, "feature")}.")
          for (i <- 0 to params.maxFeatures) {
            if (dataIter.hasNext) fc.add(dataIter.next())
          }
        } else {
          dataIter.foreach(fc.add)
        }
      }
      logger.info("Write data")
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
  class ConvertParameters extends BaseExportCommands
    with BaseBinaryExportParameters
    with OptionalFeatureTypeSpecParam
    with OptionalCQLFilterParam
    with InputFileParams {}
}
