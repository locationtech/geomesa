/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io._

import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory.DelimitedOptions
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.IndexedSeq

class DelimitedTextConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  import DelimitedTextConverterFactory._

  override protected val typeToProcess = "delimited-text"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        cacheServices: Map[String, EnrichmentCache],
                                        parseOpts: ConvertParseOpts): DelimitedTextConverter = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._

    val baseFormat = conf.getString("format").toUpperCase match {
      case "CSV" | "DEFAULT"          => CSVFormat.DEFAULT
      case "EXCEL"                    => CSVFormat.EXCEL
      case "MYSQL"                    => CSVFormat.MYSQL
      case "TDF" | "TSV" | "TAB"      => CSVFormat.TDF
      case "RFC4180"                  => CSVFormat.RFC4180
      case "QUOTED"                   => QUOTED
      case "QUOTE_ESCAPE"             => QUOTE_ESCAPE
      case "QUOTED_WITH_QUOTE_ESCAPE" => QUOTED_WITH_QUOTE_ESCAPE
      case _ => throw new IllegalArgumentException("Unknown delimited text format")
    }

    val format = formatOptions.foldLeft(baseFormat) { case (fmt, (name, fn)) =>
      conf.getStringOpt(s"options.$name") match {
        case None => fmt
        case Some(o) if o.length == 1 => fn.apply(fmt, o.toCharArray()(0))
        case Some(o) => throw new IllegalArgumentException(s"$name must be a single character: $o")
      }
    }

    val opts = new DelimitedOptions()
    conf.getIntOpt("options.skip-lines").foreach(s => opts.skipLines = s)
    conf.getIntOpt("options.pipe-size").foreach(p => opts.pipeSize = p)

    new DelimitedTextConverter(format, sft, idBuilder, fields, userDataBuilder, cacheServices, opts, parseOpts)
  }
}

object DelimitedTextConverterFactory {

  private val QUOTED                   = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  private val QUOTE_ESCAPE             = CSVFormat.DEFAULT.withEscape('"')
  private val QUOTED_WITH_QUOTE_ESCAPE = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  private val formatOptions: Seq[(String, (CSVFormat, Char) => CSVFormat)] = Seq(
    ("quote",     (f, c) => f.withQuote(c)),
    ("escape",    (f, c) => f.withEscape(c)),
    ("delimiter", (f, c) => f.withDelimiter(c))
  )

  class DelimitedOptions(var skipLines: Int = 0, var pipeSize: Int = 16 * 1024)
}

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field],
                             val userDataBuilder: Map[String, Expr],
                             val caches: Map[String, EnrichmentCache],
                             val options: DelimitedOptions,
                             val parseOpts: ConvertParseOpts)
  extends LinesToSimpleFeatureConverter {

  override def processInput(is: Iterator[String], ec: EvaluationContext): Iterator[SimpleFeature] = {
    ec.counter.incLineCount(options.skipLines)
    super.processInput(is.drop(options.skipLines), ec)
  }

  override def fromInputType(string: String): Seq[Array[Any]] = {
    if (string == null || string.isEmpty) {
      throw new IllegalArgumentException("Invalid input (empty)")
    }
    val rec = format.parse(new StringReader(string)).iterator().next()
    val len = rec.size()
    val ret = Array.ofDim[Any](len + 1)
    ret(0) = string
    var i = 0
    while (i < len) {
      ret(i+1) = rec.get(i)
      i += 1
    }
    Seq(ret)
  }
}
