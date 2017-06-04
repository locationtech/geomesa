/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.{FileInputStream, _}
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.io.IOUtils
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.DataFeatureCollection
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureConverter, SimpleFeatureConverters, Transformers}
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export._
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    implicit val timing = new Timing
    val count = profile(convertAndExport())
    Command.user.info(s"Conversion complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${timing.time}ms${count.map(c => s" for $c features").getOrElse("")}")
  }

  private def convertAndExport(): Option[Long] = {
    import ConvertCommand.{getConverter, getExporter, loadFeatureCollection}

    import scala.collection.JavaConversions._

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: ${SimpleFeatureTypes.encodeType(sft)}")

    val converter = getConverter(params, sft)
    val filter = Option(params.cqlFilter).map { f =>
      Command.user.debug(s"Applying CQL filter $f")
      ECQL.toFilter(f)
    }
    val ec = converter.createEvaluationContext(Map("inputFilePath" -> ""))
    val fc = loadFeatureCollection(params.files, converter, ec, filter, Option(params.maxFeatures).map(_.intValue))

    val exporter = getExporter(params, sft, fc)

    try {
      val count = exporter.export(fc)
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

  def getExporter(params: ConvertParameters, sft: SimpleFeatureType, fc: SimpleFeatureCollection): FeatureExporter = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val outFmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException("Unable to parse output file type.")
    }

    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    lazy val writer: Writer = ExportCommand.getWriter(params)
    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val hints = {
      val q = new Query("")
      Option(params.hints).foreach { hints =>
        q.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
        ViewParams.setHints(sft, q, null)
      }
      q.getHints
    }
    lazy val arrowDictionaries: Map[String, Seq[AnyRef]] = {
      val attributes = hints.getArrowDictionaryFields
      if (attributes.isEmpty) { Map.empty } else {
        val values = attributes.map(a => a -> scala.collection.mutable.HashSet.empty[AnyRef])
        SelfClosingIterator(fc.features()).foreach(f => values.foreach { case (a, v) => v.add(f.getAttribute(a))})
        values.map { case (attribute, value) => attribute -> value.toSeq }.toMap
      }
    }

    outFmt match {
      case Csv | Tsv      => new DelimitedExporter(writer, outFmt, None, !params.noHeader)
      case Shp            => new ShapefileExporter(ExportCommand.checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml            => new GmlExporter(outputStream)
      case Avro           => new AvroExporter(sft, outputStream, avroCompression)
      case Bin            => new BinExporter(hints, outputStream)
      case Arrow          => new ArrowExporter(hints, outputStream, arrowDictionaries)
      case _              => throw new ParameterException(s"Format $outFmt is not supported.")
    }
  }

  def loadFeatureCollection(files: Iterable[String],
                            converter: SimpleFeatureConverter[Any],
                            ec: EvaluationContext,
                            filter: Option[Filter],
                            maxFeatures: Option[Int]): SimpleFeatureCollection = {
    def convert(): Iterator[SimpleFeature] = {
      val features = files.iterator.flatMap { file =>
        ec.set(ec.indexOf("inputFilePath"), file)
        val is = PathUtils.handleCompression(new FileInputStream(file), file)
        converter.process(is, ec)
      }
      val filtered = filter.map(f => features.filter(f.evaluate)).getOrElse(features)
      val limited = maxFeatures.map(filtered.take).getOrElse(filtered)
      limited
    }

    new DataFeatureCollection("features", converter.targetSFT) {
      import scala.collection.JavaConversions._
      override def getBounds: ReferencedEnvelope = {
        val bounds = new ReferencedEnvelope()
        convert().foreach(f => bounds.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal))
        bounds
      }
      override def getCount: Int = convert().length
      override protected def openIterator(): java.util.Iterator[SimpleFeature] = convert()
    }
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FileExportParams with InputFilesParam with OptionalTypeNameParam
    with RequiredFeatureSpecParam with RequiredConverterConfigParam
}
