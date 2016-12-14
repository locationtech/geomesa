/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.text

import java.io._
import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert.{AbstractSimpleFeatureConverterFactory, Field, LinesToSimpleFeatureConverter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.immutable.IndexedSeq

class DelimitedTextConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  override protected val typeToProcess = "delimited-text"

  val QUOTED                    = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  val QUOTE_ESCAPE              = CSVFormat.DEFAULT.withEscape('"')
  val QUOTED_WITH_QUOTE_ESCAPE  = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        validating: Boolean): DelimitedTextConverter = {
    var baseFmt = conf.getString("format").toUpperCase match {
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

    import org.locationtech.geomesa.utils.conf.ConfConversions._
    val opts = {
      val o = "options"
      val dOpts = new DelimitedOptions()
      conf.getIntOpt(s"$o.skip-lines").foreach(s => dOpts.skipLines = s)
      conf.getIntOpt(s"$o.pipe-size").foreach(p => dOpts.pipeSize = p)
      dOpts
    }

    conf.getStringOpt("options.quote").foreach { q =>
      require(q.length == 1, "Quote must be a single character")
      baseFmt = baseFmt.withQuote(q.toCharArray()(0))
    }

    conf.getStringOpt("options.escape").foreach { q =>
      require(q.length == 1, "Escape must be a single character")
      baseFmt = baseFmt.withEscape(q.toCharArray()(0))
    }

    new DelimitedTextConverter(baseFmt, sft, idBuilder, fields, userDataBuilder, opts, validating)
  }
}

class DelimitedOptions(var skipLines: Int = 0, var pipeSize: Int = 16 * 1024)

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field],
                             val userDataBuilder: Map[String, Expr],
                             val options: DelimitedOptions,
                             val validating: Boolean)
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
