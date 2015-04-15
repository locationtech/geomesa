package org.locationtech.geomesa.stream.generic

import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.net.DefaultSocketFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.stream.SimpleFeatureStreamSource
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class GenericSimpleFeatureStreamSourceTest extends Specification  {

  "GenericSimpleFeatureStreamSource" should {

    val conf = ConfigFactory.parseString(
      """
        |{
        |  type         = "generic"
        |  source-route = "netty4:tcp://localhost:5899?textline=true"
        |  sft          = {
        |                   type-name = "testdata"
        |                   fields = [
        |                     { name = "label",     type = "String" }
        |                     { name = "geom",      type = "Point",  index = true, srid = 4326, default = true }
        |                     { name = "dtg",       type = "Date",   index = true }
        |                   ]
        |                 }
        |  threads      = 4
        |  converter    = {
        |                   id-field = "md5(string2bytes($0))"
        |                   type = "delimited-text"
        |                   format = "DEFAULT"
        |                   fields = [
        |                     { name = "label",     transform = "trim($1)" }
        |                     { name = "geom",      transform = "point($2::double, $3::double)" }
        |                     { name = "dtg",       transform = "datetime($4)" }
        |                   ]
        |                 }
        |}
      """.stripMargin
    )

    "be built from a conf" >> {
      val source = SimpleFeatureStreamSource.buildSource(conf)
      source.init()
      source must not beNull

      val url = Resources.getResource("testdata.tsv")
      val lines = Resources.readLines(url, StandardCharsets.UTF_8)
      val socketFactory = new DefaultSocketFactory
      Future {
        val socket = socketFactory.createSocket("localhost", 5899)
        val os = socket.getOutputStream
        IOUtils.writeLines(lines, IOUtils.LINE_SEPARATOR_UNIX, os)
        os.flush()
        // wait for data to arrive at the server
        Thread.sleep(4000)
        os.close()
      }

      var i = 0
      val iter = new Iterator[SimpleFeature] {
        override def hasNext: Boolean = true
        override def next() = {
          var ret: SimpleFeature = null
          while(ret == null) {
            ret = source.next
          }
          i+=1
          ret
        }
      }
      val result = iter.take(lines.length).toList
      result.length must be equalTo lines.length
    }
  }
}