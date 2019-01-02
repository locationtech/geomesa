/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, URLSftProvider}
import org.mortbay.jetty.handler.AbstractHandler
import org.mortbay.jetty.{Request, Server}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class URLConfigProviderTest extends Specification {

  "URLConfigProvider" should {

    "pull in concatenated configs from a url" >> {

      class ConfHandler extends AbstractHandler {
        override def handle(s: String, req: HttpServletRequest, resp: HttpServletResponse, i: Int): Unit = {
          resp.setContentType("text/plain;charset=utf-8")
          resp.setStatus(HttpServletResponse.SC_OK)
          req.asInstanceOf[Request].setHandled(true)
          resp.getWriter.write(
            """
              |geomesa {
              |  sfts {
              |    "example-csv-url" = {
              |      attributes = [
              |        { name = "id",       type = "Integer",      index = false                             }
              |        { name = "name",     type = "String",       index = true                              }
              |        { name = "age",      type = "Integer",      index = false                             }
              |        { name = "lastseen", type = "Date",         index = true                              }
              |        { name = "friends",  type = "List[String]", index = true                              }
              |        { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
              |      ]
              |    }
              |  }
              |  converters {
              |    "example-csv-url" = {
              |      type   = "delimited-text",
              |      format = "CSV",
              |      options {
              |        skip-lines = 1
              |      },
              |      id-field = "toString($id)",
              |      fields = [
              |        { name = "id",       transform = "$1::int"                 }
              |        { name = "name",     transform = "$2::string"              }
              |        { name = "age",      transform = "$3::int"                 }
              |        { name = "lastseen", transform = "date('yyyy-MM-dd', $4)"  }
              |        { name = "friends",  transform = "parseList('string', $5)" }
              |        { name = "lon",      transform = "$6::double"              }
              |        { name = "lat",      transform = "$7::double"              }
              |        { name = "geom",     transform = "point($lon, $lat)"       }
              |      ]
              |    }
              |  }
              |}
              |geomesa {
              |  sfts {
              |    "example-csv-url2" = {
              |      attributes = [
              |        { name = "id",       type = "Integer",      index = false                             }
              |        { name = "name",     type = "String",       index = true                              }
              |        { name = "age",      type = "Integer",      index = false                             }
              |        { name = "lastseen", type = "Date",         index = true                              }
              |        { name = "friends",  type = "List[String]", index = true                              }
              |        { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
              |      ]
              |    }
              |  }
              |  converters {
              |   "example-csv-url2" = {
              |      type   = "delimited-text",
              |      format = "CSV",
              |      options {
              |        skip-lines = 5
              |      },
              |      id-field = "toString($id)",
              |      fields = [
              |        { name = "id",       transform = "$1::int"                 }
              |        { name = "name",     transform = "$2::string"              }
              |        { name = "age",      transform = "$3::int"                 }
              |        { name = "lastseen", transform = "date('yyyy-MM-dd', $4)"  }
              |        { name = "friends",  transform = "parseList('string', $5)" }
              |        { name = "lon",      transform = "$6::double"              }
              |        { name = "lat",      transform = "$7::double"              }
              |        { name = "geom",     transform = "point($lon, $lat)"       }
              |      ]
              |    }
              |  }
              |}
            """.stripMargin)
        }
      }
      val jetty = new Server(0)
      jetty.setHandler(new ConfHandler())
      try {
        jetty.start()
        val port = jetty.getConnectors()(0).getLocalPort

        // Should be able to handle bad urls as well as good urls
        System.setProperty(URLConfigProvider.ConverterConfigURLs, s"http://localhost:$port/,http://fakeurl:80/foobar")
        System.setProperty(URLSftProvider.SftConfigURLs, s"http://localhost:$port/,http://fakeurl:80/foobar")
        // This ensures that the URLConfigProvider's config will re-read
        ConfigFactory.invalidateCaches()

        ConverterConfigLoader.listConverterNames must containAllOf(Seq("example-csv-url", "example-csv-url2"))
        SimpleFeatureTypeLoader.listTypeNames must containAllOf(Seq("example-csv-url", "example-csv-url2"))

        // Intentional second calls to ensure the providers is a list and can be called twice
        ConverterConfigLoader.listConverterNames must containAllOf(Seq("example-csv-url", "example-csv-url2"))
        SimpleFeatureTypeLoader.listTypeNames must containAllOf(Seq("example-csv-url", "example-csv-url2"))

        SimpleFeatureTypeLoader.sftForName("example-csv-url").isDefined must beTrue

        val configOpt = ConverterConfigLoader.configForName("example-csv-url")
        configOpt.isDefined must beTrue
        configOpt.get.getInt("options.skip-lines") mustEqual 1

        val configOpt2 = ConverterConfigLoader.configForName("example-csv-url2")
        configOpt2.isDefined must beTrue
        configOpt2.get.getInt("options.skip-lines") mustEqual 5
      }
      finally {
        jetty.stop()
        System.clearProperty(URLConfigProvider.ConverterConfigURLs)
        System.clearProperty(URLSftProvider.SftConfigURLs)
      }
    }
  }

}
