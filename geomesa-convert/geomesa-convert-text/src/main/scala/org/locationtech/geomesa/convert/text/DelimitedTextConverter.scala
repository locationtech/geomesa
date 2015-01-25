package org.locationtech.geomesa.convert.text

import java.io.{PipedReader, PipedWriter}

import com.google.common.collect.ObjectArrays
import com.typesafe.config.Config
import org.apache.commons.csv.{QuoteMode, CSVFormat}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class DelimitedTextConverterFactory extends SimpleFeatureConverterFactory[String] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "delimited-text")

  def buildConverter(conf: Config): DelimitedTextConverter = apply(conf)

  val QUOTED                    = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  val QUOTE_ESCAPE              = CSVFormat.DEFAULT.withEscape('"')
  val QUOTED_WITH_QUOTE_ESCAPE  = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  def apply(conf: Config): DelimitedTextConverter = {
    val format    = conf.getString("format") match {
      case "DEFAULT"                  => CSVFormat.DEFAULT
      case "EXCEL"                    => CSVFormat.EXCEL
      case "MYSQL"                    => CSVFormat.MYSQL
      case "TDF"                      => CSVFormat.TDF
      case "RFC4180"                  => CSVFormat.RFC4180
      case "QUOTED"                   => QUOTED
      case "QUOTE_ESCAPE"             => QUOTE_ESCAPE
      case "QUOTED_WITH_QUOTE_ESCAPE" => QUOTED_WITH_QUOTE_ESCAPE
      case _ => throw new IllegalArgumentException("Unknown delimited text format")
    }
    val fields    = buildFields(conf.getConfigList("fields"))
    val targetSFT = findTargetSFT(conf.getString("type-name"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new DelimitedTextConverter(format, targetSFT, idBuilder, fields)
  }
}

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field])
  extends ToSimpleFeatureConverter[String] {

  var curString: String = null
  val writer = new PipedWriter()
  val reader = new PipedReader(writer)
  val parser = format.parse(reader).iterator()

  def fromInputType(string: String): Array[Any] = {
    writer.write(string)
    writer.write(format.getRecordSeparator)
    val rec = parser.next()
    val splitIter = (0 until inputFields.length).map { i => rec.get(i) }.toArray
    ObjectArrays.concat(string, splitIter).asInstanceOf[Array[Any]]
  }

  override def close(): Unit = {
    writer.close()
    reader.close()
  }
}
