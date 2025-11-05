/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mortbay.jetty.bio.SocketConnector
import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler}
import org.mortbay.jetty.{Handler, Request, Server}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import java.io.ByteArrayInputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

@RunWith(classOf[JUnitRunner])
class AvroSchemaRegistryConverterTest extends Specification with AvroSchemaRegistryUtils with BeforeAfterAll with LazyLogging {

  sequential

  private val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
  private var jetty: Server = _
  private var port: Int = -1

  lazy val config = {
    val registry = ConfigFactory.parseString(s"""{schema-registry = "http://localhost:$port"}""")
    val conf = ConfigFactory.parseString(
      """{
        |  type        = "avro-schema-registry"
        |  sft         = "testsft"
        |  id-field    = "uuid()"
        |  fields = [
        |    { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
        |    { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
        |    { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
        |    { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
        |    { name = "geom", transform = "point($lon, $lat)" }
        |    { name = "extra", transform = "avroPath($tobj, '/kvmap[$k=extra]/extra')" }
        |  ]
        |}""".stripMargin
    )
    conf.withFallback(registry)
  }

  override def beforeAll(): Unit = {
    jetty = new Server()
    val connector = new SocketConnector()
    connector.setPort(0)
    jetty.addConnector(connector)

    val context1 = new ContextHandler()
    context1.setContextPath("/schemas/ids/1")
    jetty.setHandler(context1)
    context1.setHandler(new SchemaRegistryHandlerv1())
    val context2 = new ContextHandler()
    context2.setContextPath("/schemas/ids/2")
    jetty.setHandler(context2)
    context2.setHandler(new SchemaRegistryHandlerv2())
    jetty.setHandlers(Array[Handler](context1, context2))

    jetty.start()
    port = connector.getLocalPort
  }

  override def afterAll(): Unit = {
    if (jetty != null) {
      jetty.stop()
    }
  }

  "AvroSchemaRegistryConverter should" should {
    "properly convert a GenericRecord to a SimpleFeature with a schema registry" >> {
      WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext()
        val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
        res must haveLength(3)
        val sf = res(0)
        val sf1 = res(1)
        sf.getAttributeCount must be equalTo 3
        sf.getAttribute("dtg") must not(beNull)
        sf.getAttribute("extra") must beNull
        sf1.getAttribute("extra") must be equalTo "TEST"

        ec.failure.getCount mustEqual 0L
        ec.stats.failure(0) mustEqual 0L
        ec.stats.success(0) mustEqual 3L
        ec.success.getCount mustEqual 3L
        ec.line mustEqual 3L
      }
    }
    "properly convert a GenericRecord to a SimpleFeature with a schema registry, providing record bytes" >> {
      val conf = ConfigFactory.parseString("""{id-field:"murmurHash3($0)"}""").withFallback(config)
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
        res must haveLength(3)
        val sf = res(0)
        val sf1 = res(1)
        sf.getID mustEqual "e54516336684966c4d37605046f2981f"
        sf.getAttributeCount must be equalTo 3
        sf.getAttribute("dtg") must not(beNull)
        sf.getAttribute("extra") must beNull
        sf1.getAttribute("extra") must be equalTo "TEST"

        ec.failure.getCount mustEqual 0L
        ec.stats.failure(0) mustEqual 0L
        ec.stats.success(0) mustEqual 3L
        ec.success.getCount mustEqual 3L
        ec.line mustEqual 3L
      }
    }
  }

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
}
