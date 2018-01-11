/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.Writer
import java.time.{Instant, ZoneOffset}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.tools.export.ExportCommand.ExportAttributes
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class DelimitedExporter(writer: Writer, format: DataFormat, attributes: Option[ExportAttributes], withHeader: Boolean)
    extends FeatureExporter with LazyLogging {

  import scala.collection.JavaConversions._

  private val printer = format match {
    case DataFormats.Csv => CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL).print(writer)
    case DataFormats.Tsv => CSVFormat.TDF.withQuoteMode(QuoteMode.MINIMAL).print(writer)
  }

  private val withId = attributes.forall(_.fid)
  private var names: Seq[String] = _

  override def start(sft: SimpleFeatureType): Unit = {
    names = attributes.map(_.names).getOrElse(sft.getAttributeDescriptors.map(_.getLocalName))

    // write out a header line
    if (withHeader) {
      if (withId) {
        printer.print("id")
      }
      names.foreach(name => printer.print(SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor(name))))
      printer.println()
      printer.flush()
    }
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach { sf =>
      if (withId) {
        printer.print(sf.getID)
      }
      // retrieve values by name, index doesn't always correspond correctly due to geometry being added back in
      names.foreach(name => printer.print(stringify(sf.getAttribute(name))))
      printer.println()

      count += 1
      if (count % 10000 == 0) {
        logger.debug(s"wrote $count features")
      }
    }

    printer.flush()

    logger.debug(s"Exported $count features")
    Some(count)
  }

  override def close(): Unit = printer.close()

  private def stringify(o: Any): String = {
    import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
    o match {
      case null                   => ""
      case g: Geometry            => WKTUtils.write(g)
      case d: Date                => GeoToolsDateFormat.format(Instant.ofEpochMilli(d.getTime).atZone(ZoneOffset.UTC))
      case l: java.util.List[_]   => l.map(stringify).mkString(",")
      case m: java.util.Map[_, _] => m.map { case (k, v) => s"${stringify(k)}->${stringify(v)}"}.mkString(",")
      case _                      => o.toString
    }
  }
}


