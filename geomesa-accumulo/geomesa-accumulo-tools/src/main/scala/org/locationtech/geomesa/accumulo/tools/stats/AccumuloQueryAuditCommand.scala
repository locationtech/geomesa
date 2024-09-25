/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.stats

import com.beust.jcommander.{IParameterValidator, Parameter, ParameterException, Parameters}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.audit.AccumuloAuditReader
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.stats.AccumuloQueryAuditCommand.{AccumuloQueryAuditParams, CsvWriter, JsonWriter}
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.{FilterHelper, andOption}
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.tools.utils.ParameterConverters.{DateConverter, FilterConverter}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.{Instant, ZoneOffset}
import java.util.Date

/**
 * Queries audit logs
 */
class AccumuloQueryAuditCommand extends AccumuloDataStoreCommand {

  override val name = "query-audit-logs"
  override val params = new AccumuloQueryAuditParams()

  override def execute(): Unit = withDataStore(exec)

  private def exec(ds: AccumuloDataStore): Unit = {
    if (ds.getSchema(params.featureName) == null) {
      throw new ParameterException(s"SimpleFeatureType '${params.featureName}' does not exist in the data store")
    }

    val cql = Option(params.cqlFilter).filter(_ != Filter.INCLUDE)
    val dateRanges = {
      import FilterHelper.ff
      val startFilter = Option(params.start).map(d => ff.greaterOrEqual(ff.property("end"), ff.literal(d)))
      val endFilter = Option(params.end).map(d => ff.less(ff.property("end"), ff.literal(d)))
      val dateFilter = andOption(cql.toSeq ++ startFilter ++ endFilter)
      val intervals = dateFilter.map { f =>
        val intervals = FilterHelper.extractIntervals(f, "end")
        if (intervals.disjoint) {
          throw new ParameterException(s"Invalid disjoint date filter: ${ECQL.toCQL(f)}")
        }
        intervals.values
      }
      val bounds = intervals.toSeq.flatten
      if (bounds.isEmpty) {
        Seq((Instant.EPOCH.atZone(ZoneOffset.UTC), Instant.now().atZone(ZoneOffset.UTC)))
      } else {
        val now = Instant.now().atZone(ZoneOffset.UTC)
        bounds.map { bound =>
          val start = bound.lower.value.map(d => if (bound.lower.inclusive) { d } else { d.plusNanos(1000000) })
          val end = bound.upper.value.map(d => if (bound.upper.exclusive) { d } else { d.plusNanos(1000000) })
          (start.getOrElse(Instant.EPOCH.atZone(ZoneOffset.UTC)), end.getOrElse(now))
        }
      }
    }

    WithClose(new AccumuloAuditReader(ds)) { reader=>
      val writer = if ("json".equalsIgnoreCase(params.outputFormat)) { new JsonWriter() } else { new CsvWriter() }
      writer.header().foreach(h => Command.output.info(h))
      dateRanges.foreach { case (start, end) =>
        WithClose(reader.getQueryEvents(params.featureName, (start, end))) { events =>
          val out = cql match {
            case None => events
            case Some(f) => events.filter(e => f.evaluate(AccumuloQueryAuditCommand.toFeature(e)))
          }
          out.foreach { event =>
            Command.output.info(writer.output(event))
          }
        }
      }
    }
  }
}

object AccumuloQueryAuditCommand {

  private final val CqlDescription =
    "CQL predicate to filter log entries. Schema is: " +
      "user:String,filter:String,hints:String:json=true,metadata:String:json=true,start:Date,end:Date,planTimeMillis:Long,scanTimeMillis:Long,hits:Long"

  // note: the cql description needs to be a final string constant to work with annotations, so we extract the spec
  // from there instead of building it up as we might normally do
  private val Spec = CqlDescription.substring(CqlDescription.indexOf(": ") + 2)
  private val Sft = SimpleFeatureTypes.createType("audit-logs", Spec)

  /**
   * Convert an audit event to a simple feature
   *
   * @param event event
   * @return
   */
  private def toFeature(event: QueryEvent): SimpleFeature = {
    val hints = AuditWriter.Gson.toJson(event.hints)
    val metadata = AuditWriter.Gson.toJson(event.metadata)
    val start = if (event.start == -1) { null } else { new Date(event.start) }
    val end = new Date(event.end)
    val planTime = Long.box(event.planTime)
    val scanTime = Long.box(event.scanTime)
    val hits = Long.box(event.hits)
    new ScalaSimpleFeature(Sft, "", Array(event.user, event.filter, hints, metadata, start, end, planTime, scanTime, hits))
  }

  @Parameters(commandDescription = "Search query audit logs for a GeoMesa feature type")
  class AccumuloQueryAuditParams extends AccumuloDataStoreParams with RequiredTypeNameParam {

    @Parameter(
      names = Array("-b", "--begin"),
      description = "Lower bound (inclusive) on the date of log entries to return, in ISO 8601 format",
      converter = classOf[DateConverter])
    var start: Date = _

    @Parameter(
      names = Array("-e", "--end"),
      description = "Upper bound (exclusive) on the date of log entries to return, in ISO 8601 format",
      converter = classOf[DateConverter])
    var end: Date = _

    @Parameter(
      names = Array("--output-format"),
      description = "Output format, either 'json' or 'csv'",
      validateWith = Array(classOf[OutputFormatValidator]))
    var outputFormat: String = "csv"

    @Parameter(
      names = Array("-q", "--cql"),
      description = CqlDescription,
      converter = classOf[FilterConverter])
    var cqlFilter: Filter = _
  }

  class OutputFormatValidator extends IParameterValidator {
    override def validate(name: String, value: String): Unit =
      Seq("json", "csv").find(_.equalsIgnoreCase(value)).getOrElse(throw new ParameterException(s"Invalid output format '$value'"))
  }

  private trait OutputWriter {
    def header(): Option[String]
    def output(event: QueryEvent): String
  }

  private class CsvWriter extends OutputWriter {
    private val out = new java.lang.StringBuilder()
    // note: don't need to close the printer, as it just closes the underlying stream (i.e. our StringBuilder)
    private val printer = new CSVPrinter(out, CSVFormat.DEFAULT)

    override def header(): Option[String] = Some(Spec.replaceAll(":\\w+", ""))
    override def output(event: QueryEvent): String = {
      printer.println() // reset new record
      out.setLength(0)
      printer.print(event.user)
      printer.print(event.filter)
      printer.print(AuditWriter.Gson.toJson(event.hints))
      printer.print(AuditWriter.Gson.toJson(event.metadata))
      printer.print(if (event.start == -1) { null } else { DateParsing.formatMillis(event.start) })
      printer.print(DateParsing.formatMillis(event.end))
      printer.print(Long.box(event.planTime))
      printer.print(Long.box(event.scanTime))
      printer.print(Long.box(event.hits))
      // note: we don't want to end the record as that inserts a newline, which our logger output is already doing
      out.toString
    }
  }

  private class JsonWriter extends OutputWriter {
    override def header(): Option[String] = None
    override def output(event: QueryEvent): String = {
      val model = new java.util.LinkedHashMap[String, AnyRef]()
      model.put("user", event.user)
      model.put("filter", event.filter)
      model.put("hints", event.hints)
      model.put("metadata", event.metadata)
      if (event.start != -1) {
        model.put("start", DateParsing.formatMillis(event.start))
      }
      model.put("end", DateParsing.formatMillis(event.end))
      model.put("planTimeMillis", Long.box(event.planTime))
      model.put("scanTimeMillis", Long.box(event.scanTime))
      model.put("hits", Long.box(event.hits))
      AuditWriter.Gson.toJson(model)
    }
  }
}
