/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.testing

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.convert2.transforms.ScriptingFunctionFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScriptingFunctionFactoryTest extends Specification {

  lazy val paths = {
    val basedir =
      new File(ClassLoader.getSystemResource("geomesa-convert-scripts/hello.js").toURI)
          .getParentFile.getParentFile.getParentFile.getParent
    Seq(
      s"$basedir/src/test/static", // directory
      s"$basedir/src/test/static2", // directory that doesn't exist
      s"$basedir/src/test/static3/whatsup.js", // file that exists
      s"$basedir/src/test/static3/random.js" // file that doesn't exists
    )
  }

  "ScriptingFunctionFactory " should {

    "load functions" >> {
      ScriptingFunctionFactory.ConvertScriptsPath.threadLocalValue.set(paths.mkString(":"))
      try {
        new ScriptingFunctionFactory().functions.flatMap(_.names) must
            containAllOf(Seq("js:hello", "js:gbye", "js:whatsup"))
      } finally {
        ScriptingFunctionFactory.ConvertScriptsPath.threadLocalValue.remove()
      }
    }

    "execute functions" >> {
      implicit val ec: EvaluationContext = EvaluationContext.empty
      ScriptingFunctionFactory.ConvertScriptsPath.threadLocalValue.set(paths.mkString(":"))
      try {
        val sff = new ScriptingFunctionFactory
        val hello = sff.functions.find(_.names.contains("js:hello")).head
        hello.eval(Array("geomesa")) mustEqual "hello: geomesa"
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.eval(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.eval(Array("geomesa")) mustEqual "whatsup: geomesa"
      } finally {
        ScriptingFunctionFactory.ConvertScriptsPath.threadLocalValue.remove()
      }
    }

    "work in a transformer" >> {

      val data =
        """
          |1,hello,45.0,45.0
          |2,world,90.0,90.0
          |willfail,hello
        """.stripMargin.getBytes(StandardCharsets.UTF_8)

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "oneup",    transform = "$1::string" },
          |     { name = "phrase",   transform = "js:hello($2)" },
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
      val converter = SimpleFeatureConverter(sft, conf)

      val res = converter.process(new ByteArrayInputStream(data)).toList
      converter.close()

      res must haveLength(2)
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello: hello"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello: world"
    }
  }
}
