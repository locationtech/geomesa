/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.index.stats.RunnableStats
import org.locationtech.geomesa.tools.export.ConvertCommand.ConvertParameters
import org.locationtech.geomesa.tools.export.ExportCommand.{ChunkedExporter, ExportOptions, ExportParams, Exporter}
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.{ConverterConfigParam, OptionalFeatureSpecParam, OptionalInputFormatParam, OptionalTypeNameParam, _}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Stat}
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Conversion complete to ${Option(params.file).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(c => s" for $c features").getOrElse("")}")

    profile(complete _)(convertAndExport())
  }

  private def convertAndExport(): Option[Long] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConverters._

    if (params.files.isEmpty && !StdInHandle.isAvailable) {
      throw new ParameterException("Missing option: <files>... is required")
    }

    val inputs = params.files.asScala
    val format = IngestCommand.getDataFormat(params, inputs)

    // use .get to re-throw the exception if we fail
    IngestCommand.getSftAndConverter(params, inputs, format, None).get.flatMap { case (sft, config) =>
      val files = if (inputs.isEmpty) { StdInHandle.available().iterator } else {
        inputs.iterator.flatMap(PathUtils.interpretPath)
      }
      WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(""))
        val query = ExportCommand.createQuery(sft, params)
        val exporter = Option(params.chunkSize) match {
          case None    => new Exporter(ExportOptions(params), query.getHints, Map.empty)
          case Some(c) => new ChunkedExporter(ExportOptions(params), query.getHints, Map.empty, c)
        }
        try {
          exporter.start(query.getHints.getReturnSft)
          val count = WithClose(ConvertCommand.convertFeatures(files, converter, ec, query))(exporter.export)
          val records = ec.line - (if (params.noHeader) { 0 } else { params.files.size })
          Command.user.info(s"Converted ${getPlural(records, "line")} "
              + s"with ${getPlural(ec.success.getCount, "success", "successes")} "
              + s"and ${getPlural(ec.failure.getCount, "failure")}")
          count
        } finally {
          exporter.close()
        }
      }
    }
  }
}

object ConvertCommand extends LazyLogging {

  /**
    * Convert features
    *
    * @param files inputs
    * @param converter converter
    * @param ec evaluation context
    * @param query query used to filter/transform inputs
    * @return
    */
  def convertFeatures(
      files: Iterator[FileHandle],
      converter: SimpleFeatureConverter,
      ec: EvaluationContext,
      query: Query): CloseableIterator[SimpleFeature] = {

    import org.locationtech.geomesa.convert.EvaluationContext.RichEvaluationContext
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    def convert(): CloseableIterator[SimpleFeature] = CloseableIterator(files).flatMap { file =>
      file.open.flatMap { case (name, is) =>
        ec.setInputFilePath(name.getOrElse(file.path))
        converter.process(is, ec)
      }
    }

    def filter(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      if (query.getFilter == Filter.INCLUDE) { iter } else { iter.filter(query.getFilter.evaluate) }

    def limit(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      query.getHints.getMaxFeatures match {
        case None    => iter
        case Some(m) => iter.take(m)
      }
    }

    def transform(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      import org.locationtech.geomesa.filter.filterToString

      val stats = new RunnableStats(null) {
        override protected def query[T <: Stat](sft: SimpleFeatureType, ignored: Filter, stats: String) : Option[T] = {
          val stat = Stat(sft, stats).asInstanceOf[T]
          try {
            WithClose(limit(filter(convert())))(_.foreach(stat.observe))
            Some(stat)
          } catch {
            case e: Exception =>
              logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(ignored)}'", e)
              None
          }
        }
      }
      val hook = Some(ArrowDictionaryHook(stats, Option(query.getFilter).filter(_ != Filter.INCLUDE)))
      LocalQueryRunner.transform(converter.targetSft, iter, query.getHints.getTransform, query.getHints, hook)
    }

    transform(limit(filter(convert())))
  }

  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends ExportParams with OptionalInputFormatParam with OptionalTypeNameParam
      with OptionalFeatureSpecParam with ConverterConfigParam with OptionalForceParam
}
