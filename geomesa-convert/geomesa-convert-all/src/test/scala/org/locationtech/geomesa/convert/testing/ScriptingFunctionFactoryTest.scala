/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.testing

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.convert2.transforms.ScriptingFunctionFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets

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
        hello.apply(Array("geomesa")) mustEqual "hello: geomesa"
<<<<<<< HEAD
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.apply(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.apply(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
        hello.eval(Array("geomesa")) mustEqual "hello: geomesa"
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.apply(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.apply(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
        whatsup.eval(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
        hello.eval(Array("geomesa")) mustEqual "hello: geomesa"
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.apply(Array("geomesa")) mustEqual "goodbye: geomesa"
        gbye.eval(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.apply(Array("geomesa")) mustEqual "whatsup: geomesa"
        whatsup.eval(Array("geomesa")) mustEqual "whatsup: geomesa"
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
        hello.eval(Array("geomesa")) mustEqual "hello: geomesa"
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.apply(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.apply(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
        whatsup.eval(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
        hello.eval(Array("geomesa")) mustEqual "hello: geomesa"
        val gbye = sff.functions.find(_.names.contains("js:gbye")).head
        gbye.apply(Array("geomesa")) mustEqual "goodbye: geomesa"
        gbye.eval(Array("geomesa")) mustEqual "goodbye: geomesa"
        val whatsup = sff.functions.find(_.names.contains("js:whatsup")).head
        whatsup.apply(Array("geomesa")) mustEqual "whatsup: geomesa"
        whatsup.eval(Array("geomesa")) mustEqual "whatsup: geomesa"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
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
