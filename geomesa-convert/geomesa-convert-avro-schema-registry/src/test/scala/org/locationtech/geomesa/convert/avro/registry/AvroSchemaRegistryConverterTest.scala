/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import java.io.ByteArrayInputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mortbay.jetty.{Handler, Request, Server}
import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSchemaRegistryConverterTest extends Specification with AvroSchemaRegistryUtils with LazyLogging {

  sequential

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "AvroSchemaRegistryConverter should" should {
    class SchemaRegistryHandlerv1 extends AbstractHandler {

      override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, i: Int): Unit = {
        response.setContentType("application/json")
        response.setStatus(HttpServletResponse.SC_OK)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.write(
          """
            |{"schema":"{\"namespace\": \"org.locationtech\",\"type\": \"record\",\"name\": \"CompositeMessage\",\"fields\": [{ \"name\": \"content\",\"type\": [{\"name\": \"TObj\",\"type\": \"record\",\"fields\": [{\"name\": \"kvmap\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"kvpair\",\"type\": \"record\",\"fields\": [{ \"name\": \"k\", \"type\": \"string\" },{ \"name\": \"v\", \"type\": [\"string\", \"double\", \"int\", \"null\"] }]}}}]},{\"name\": \"OtherObject\",\"type\": \"record\",\"fields\": [{ \"name\": \"id\", \"type\": \"int\"}]}]}]}"}
          """.stripMargin
        )
      }
    }

    class SchemaRegistryHandlerv2 extends AbstractHandler {
      override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, i: Int): Unit = {
        response.setContentType("application/json")
        response.setStatus(HttpServletResponse.SC_OK)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.write(
          """
            |{"schema":"{\"namespace\": \"org.locationtech2\",\"type\": \"record\",\"name\": \"CompositeMessage\",\"fields\": [{ \"name\": \"content\",\"type\": [{\"name\": \"TObj\",\"type\": \"record\",\"fields\": [{\"name\": \"kvmap\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"kvpair\",\"type\": \"record\",\"fields\": [{ \"name\": \"k\", \"type\": \"string\" },{ \"name\": \"v\", \"type\": [\"string\", \"double\", \"int\", \"null\"] },{ \"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null }]}}}]},{\"name\": \"OtherObject\",\"type\": \"record\",\"fields\": [{ \"name\": \"id\", \"type\": \"int\"}]}]}]}"}
          """.stripMargin
        )
      }
    }

    val jetty = new Server(8089)
    val context1 = new ContextHandler()
    context1.setContextPath("/schemas/ids/1")
    jetty.setHandler(context1)
    context1.setHandler(new SchemaRegistryHandlerv1())
    val context2 = new ContextHandler()
    context2.setContextPath("/schemas/ids/2")
    jetty.setHandler(context2)
    context2.setHandler(new SchemaRegistryHandlerv2())
    jetty.setHandlers(Array[Handler](context1, context2))


    "properly convert a GenericRecord to a SimpleFeature with Schema Registry" >> {

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro-schema-registry"
          |   schema-registry = "http://localhost:8089"
          |   sft         = "testsft"
          |   id-field    = "uuid()"
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |     { name = "extra", transform = "avroPath($tobj, '/kvmap[$k=extra]/extra')" }
          |   ]
          | }
        """.stripMargin)

      try {
        jetty.start()

        WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
          val ec = converter.createEvaluationContext()
          val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
          res must haveLength(3)
          val sf = res(0)
          val sf1 = res(1)
          sf.getAttributeCount must be equalTo 3
          sf.getAttribute("dtg") must not(beNull)
          sf.getAttribute("extra") must beNull
          sf1.getAttribute("extra") must be equalTo "TEST"

          ec.counter.getFailure mustEqual 0L
          ec.counter.getSuccess mustEqual 3L
          ec.counter.getLineCount mustEqual 3L
        }
      } finally {
        jetty.stop()
      }
    }
  }
}
