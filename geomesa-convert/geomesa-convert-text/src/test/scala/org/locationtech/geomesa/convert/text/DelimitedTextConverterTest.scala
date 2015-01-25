package org.locationtech.geomesa.convert.text

import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DelimitedTextConverterTest extends Specification {

  "DelimitedTextConverter" should {

    val data =
      """
        |1,hello,45.0,45.0
        |2,world,90.0,90.0
      """.stripMargin

    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   type         = "delimited-text",
        |   type-name    = "testsft",
        |   format       = "DEFAULT",
        |   id-field     = "md5(string2bytes($0))",
        |   fields = [
        |     { name = "phrase", transform = "concat($1, $2)" },
        |     { name = "lat",    transform = "$3::double" },
        |     { name = "lon",    transform = "$4::double" },
        |     { name = "geom",   transform = "point($lat, $lon)" }
        |   ]
        | }
      """.stripMargin)

    "be built from a conf" >> {
      val converter = SimpleFeatureConverters.build[String](conf)
      converter must not beNull

      "and process some data" >> {
        val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
        res.size must be equalTo 2
        converter.close()
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      }
    }

    "handle tab delimited files" >> {
      val conf = ConfigFactory.parseString(
        """
          | converter = {
          |   type         = "delimited-text",
          |   type-name    = "testsft",
          |   format       = "TDF",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)
      val converter = SimpleFeatureConverters.build[String](conf)
      converter must not beNull
      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0).map(_.replaceAll(",", "\t"))).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
    }

    "handle horrible quoting and nested separators" >> {
      val conf = ConfigFactory.parseString(
        """
          | converter = {
          |   type         = "delimited-text",
          |   type-name    = "testsft",
          |   format       = "EXCEL",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      import scala.collection.JavaConversions._
      val data = Resources.readLines(Resources.getResource("messydata.csv"), StandardCharsets.UTF_8)

      val converter = SimpleFeatureConverters.build[String](conf)
      converter must not beNull
      val res = converter.processInput(data.iterator()).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello, \"foo\""
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
    }

  }
}
