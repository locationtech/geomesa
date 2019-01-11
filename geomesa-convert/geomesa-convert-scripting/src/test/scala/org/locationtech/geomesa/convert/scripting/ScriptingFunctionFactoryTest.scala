/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.scripting

import java.io.File

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.{DefaultCounter, EvaluationContextImpl, SimpleFeatureConverters}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScriptingFunctionFactoryTest extends Specification {

  sequential

  val srctestresourcesdir = new File(ClassLoader.getSystemResource("geomesa-convert-scripts/hello.js").toURI)
  val parent = srctestresourcesdir.getParentFile.getParentFile.getParentFile.getParent

  val staticPaths = Seq(
    s"$parent/src/test/static", // directory
    s"$parent/src/test/static2", // directory that doesn't exist
    s"$parent/src/test/static3/whatsup.js", // file that exists
    s"$parent/src/test/static3/random.js" // file  that doesnt exists
  )
  val path = staticPaths.mkString(":")
  System.setProperty("geomesa.convert.scripts.path", path)

  "ScriptingFunctionFactory " should {

    val sff = new ScriptingFunctionFactory

    "load functions" >> {
      sff.functions.flatMap(_.names) must contain("js:hello")
    }

    "execute functions" >> {
      implicit val ec = new EvaluationContextImpl(IndexedSeq.empty[String], Array.empty[Any], new DefaultCounter, Map.empty)
      val fn = sff.functions.find(_.names.contains("js:hello")).head
      val res = fn.eval(Array("geomesa"))
      res must beEqualTo("hello: geomesa")
    }

    "work in a transformer" >> {

      val data =
        """
          |1,hello,45.0,45.0
          |2,world,90.0,90.0
          |willfail,hello
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "oneup",    transform = "$1::string" },
          |     { name = "phrase",   transform = "js:hello($2)" },
          |     { name = "gbye",     transform = "js:gbye($2)" },
          |     { name = "whatsup",  transform = "js:whatsup($2)" },
          |     { name = "lat",      transform = "$3::double" },
          |     { name = "lon",      transform = "$4::double" },
          |     { name = "lit",      transform = "'hello'" },
          |     { name = "geom",     transform = "point($lat, $lon)" }
          |     { name = "l1",       transform = "concat($lit, $lit)" }
          |     { name = "l2",       transform = "concat($l1,  $lit)" }
          |     { name = "l3",       transform = "concat($l2,  $lit)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(
        ConfigFactory.parseString(
          """
            |{
            |  type-name = "testsft"
            |  attributes = [
            |    { name = "oneup",    type = "String", index = false },
            |    { name = "phrase",   type = "String", index = false },
            |    { name = "gbye",     type = "String", index = false },
            |    { name = "whatsup",  type = "String", index = false },
            |    { name = "lineNr",   type = "Int",    index = false },
            |    { name = "fn",       type = "String", index = false },
            |    { name = "lat",      type = "Double", index = false },
            |    { name = "lon",      type = "Double", index = false },
            |    { name = "lit",      type = "String", index = false },
            |    { name = "geom",     type = "Point",  index = true, srid = 4326, default = true }
            |  ]
            |}
          """.stripMargin
        ))
      val converter = SimpleFeatureConverters.build[String](sft, conf)

      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).isDefined)).toList
      converter.close()

      "and process some data" >> {
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello: hello"
        res(0).getAttribute("gbye").asInstanceOf[String] must be equalTo "goodbye: hello"
        res(0).getAttribute("whatsup").asInstanceOf[String] must be equalTo "whatsup: hello"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello: world"
      }

    }
  }

}
