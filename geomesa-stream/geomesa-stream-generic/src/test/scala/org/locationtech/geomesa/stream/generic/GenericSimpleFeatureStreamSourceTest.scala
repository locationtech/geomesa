/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream.generic

import java.net.{DatagramPacket, InetAddress}
import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.net.{DefaultDatagramSocketFactory, DefaultSocketFactory}
import org.junit.runner.RunWith
import org.locationtech.geomesa.stream.SimpleFeatureStreamSource
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class GenericSimpleFeatureStreamSourceTest extends Specification  {

  import scala.concurrent.ExecutionContext.Implicits.global

  "GenericSimpleFeatureStreamSource" should {

    val confString =
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

    "be built from a conf" >> {
      val source = SimpleFeatureStreamSource.buildSource(ConfigFactory.parseString(confString))
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

    "work with udp" >> {
      val port = 5898
      val udpConf = confString.replace("tcp", "udp").replace("5899", port.toString)
          .replace("textline=true", "textline=true&decoderMaxLineLength=" + Int.MaxValue)
      val source = SimpleFeatureStreamSource.buildSource(ConfigFactory.parseString(udpConf))
      source.init()
      source must not beNull

      val url = Resources.getResource("testdata.tsv")
      val lines = Resources.readLines(url, StandardCharsets.UTF_8)
      val socketFactory = new DefaultDatagramSocketFactory
      Future {
        val address = InetAddress.getByName("localhost")
        val socket = socketFactory.createDatagramSocket()
        socket.connect(address, port)

        lines.foreach { line =>
          val bytes = (line + "\n").getBytes("UTF-8")
          if (bytes.length > socket.getSendBufferSize) {
            println("Error in buffer size with line \n" + line)
          }
          val packet = new DatagramPacket(bytes, bytes.length, address, port)
          socket.send(packet)
        }
        socket.disconnect()
      }

      val iter = new Iterator[SimpleFeature] {
        override def hasNext: Boolean = true
        override def next() = {
          var ret: SimpleFeature = null
          while (ret == null) {
            Thread.sleep(10)
            ret = source.next
          }
          ret
        }
      }
      val result = iter.take(lines.length).toList
      result.length must be equalTo lines.length
    }
  }
}