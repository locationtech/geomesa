/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.text.DelimitedTextConverter
import org.locationtech.geomesa.utils.geotools.URLSftProvider
import org.mortbay.jetty.handler.AbstractHandler
import org.mortbay.jetty.{Request, Server}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class URLConfigProviderTest extends Specification {

  "URLConfigProvider" should {

    "pull in a config" >> {

      class ConfHandler extends AbstractHandler {
        override def handle(s: String, req: HttpServletRequest, resp: HttpServletResponse, i: Int): Unit = {
          resp.setContentType("text/plain;charset=utf-8")
          resp.setStatus(HttpServletResponse.SC_OK)
          req.asInstanceOf[Request].setHandled(true)
          resp.getWriter.write(
            """
              |geomesa = {
              |  sfts = [
              |    {
              |      type-name = "example-csv-fromurl"
              |      attributes = [
              |        {name = "id", type = "Integer", index = false},
              |        {name = "name", type = "String", index = true},
              |        {name = "age", type = "Integer", index = false},
              |        {name = "lastseen", type = "Date", index = true},
              |        {name = "friends", type = "List[String]", index = true},
              |        {name = "geom", type = "Point", index = true, srid = 4326, default = true}
              |      ]
              |    }
              |  ]
              |  converters = [
              |   {
              |    name = "example-csv-fromtesturl"
              |    type = "delimited-text",
              |    format = "CSV",
              |    options {
              |      skip-lines = 1
              |    },
              |    id-field = "toString($id)",
              |    fields = [
              |      {name = "id", transform = "$1::int"},
              |      {name = "name", transform = "$2::string"},
              |      {name = "age", transform = "$3::int"},
              |      {name = "lastseen", transform = "date('YYYY-MM-dd', $4)"},
              |      {name = "friends", transform = "parseList('string', $5)"},
              |      {name = "lon", transform = "$6::double"},
              |      {name = "lat", transform = "$7::double"},
              |      {name = "geom", transform = "point($lon, $lat)"}
              |    ]
              |  }
              | ]
              |}
            """.stripMargin)
        }
      }
      val jetty = new Server(0)
      jetty.setHandler(new ConfHandler())
      try {
        jetty.start()
        val port = jetty.getConnectors()(0).getLocalPort
        System.setProperty(URLConfigProvider.ConfigURLProp, s"http://localhost:$port/")
        System.setProperty(URLSftProvider.SftConfigURLs, s"http://localhost:$port/")

        val configOpt = ConverterConfigLoader.configForName("example-csv-fromtesturl")
        configOpt.isDefined must beTrue

        val config = configOpt.get
        config.getString("name") mustEqual "example-csv-fromtesturl"
        config.getInt("options.skip-lines") mustEqual 1

        val converter = SimpleFeatureConverters.build("example-csv-fromurl", "example-csv-fromtesturl")
        converter must beAnInstanceOf[DelimitedTextConverter]
      }
      finally {
        jetty.stop()
      }
    }
  }

}
