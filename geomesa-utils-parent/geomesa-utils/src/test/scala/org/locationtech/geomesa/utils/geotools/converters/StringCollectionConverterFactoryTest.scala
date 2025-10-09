/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.util.factory.{GeoTools, Hints}
import org.geotools.util.{Converter, Converters}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.UUID

@RunWith(classOf[JUnitRunner])
class StringCollectionConverterFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  private val hints = GeoTools.getDefaultHints

  step {
    hints.put(StringCollectionConverterFactory.ListTypeKey, classOf[Int])
    hints.put(StringCollectionConverterFactory.MapKeyTypeKey, classOf[String])
    hints.put(StringCollectionConverterFactory.MapValueTypeKey, classOf[Int])
  }

  private def createConverter(from: Class[_], to: Class[_], hints: Hints = hints): Converter = {
    // note: we don't call Converters.getConverterFactories(from, to) as that tries to load jai-dependent classes
    val factory = Converters.getConverterFactories(null).asScala.find(_.isInstanceOf[StringCollectionConverterFactory]).orNull
    factory must not(beNull)
    factory.createConverter(from, to, hints)
  }

  "StringCollectionConverterFactory" should {

    "create converters" >> {
      "string to list" >> {
        val converter = createConverter(classOf[String], classOf[java.util.List[_]])
        converter must not(beNull)
      }
      "string to map" >> {
        val converter = createConverter(classOf[String], classOf[java.util.Map[_, _]])
        converter must not(beNull)
      }
      "require list hints" >> {
        val converter = createConverter(classOf[String], classOf[java.util.List[_]], GeoTools.getDefaultHints)
        converter must beNull
      }
      "require map hints" >> {
        val converter = createConverter(classOf[String], classOf[java.util.Map[_, _]], GeoTools.getDefaultHints)
        converter must beNull
      }
      "return null for things it can't convert" >> {
        val converter = createConverter(classOf[String], classOf[UUID])
        converter must beNull
      }
    }

    "convert from java toString to List" >> {
      val converter = createConverter(classOf[String], classOf[java.util.List[Int]])
      val list = new java.util.ArrayList[Int]
      list.add(3)
      list.add(2)
      list.add(1)
      val converted = converter.convert(list.toString, classOf[java.util.List[Int]])
      converted must not(beNull)
      converted.size mustEqual 3
      converted.get(0) mustEqual 3
      converted.get(1) mustEqual 2
      converted.get(2) mustEqual 1
    }

    "convert from geomesa string to List" >> {
      val converter = createConverter(classOf[String], classOf[java.util.List[Int]])
      val converted = converter.convert("3,2,1", classOf[java.util.List[Int]])
      converted must not(beNull)
      converted.size mustEqual 3
      converted.get(0) mustEqual 3
      converted.get(1) mustEqual 2
      converted.get(2) mustEqual 1
    }

    "convert empty strings to empty lists" >> {
      val converter = createConverter(classOf[String], classOf[java.util.List[Int]])
      converter.convert("", classOf[java.util.List[Int]]) mustEqual java.util.List.of()
      converter.convert("[]", classOf[java.util.List[Int]]) mustEqual java.util.List.of()
    }

    "return null if can't convert to List" >> {
      val converter = createConverter(classOf[String], classOf[java.util.List[Int]])
      val converted = converter.convert("foobar", classOf[java.util.List[Int]])
      converted must beNull
    }

    "convert from java toString to Map" >> {
      val converter = createConverter(classOf[String], classOf[java.util.Map[String, Int]])
      val map = new java.util.HashMap[String, Int]
      map.put("one", 1)
      map.put("two", 2)
      val converted = converter.convert(map.toString, classOf[java.util.Map[String, Int]])
      converted must not(beNull)
      converted.size mustEqual 2
      converted.get("one") mustEqual 1
      converted.get("two") mustEqual 2
    }

    "convert from geomesa string to Map" >> {
      val converter = createConverter(classOf[String], classOf[java.util.Map[String, Int]])
      val converted = converter.convert("one->1,two->2", classOf[java.util.Map[String, Int]])
      converted must not(beNull)
      converted.size mustEqual 2
      converted.get("one") mustEqual 1
      converted.get("two") mustEqual 2
    }

    "convert empty strings to empty maps" >> {
      val converter = createConverter(classOf[String], classOf[java.util.Map[String, Int]])
      converter.convert("", classOf[java.util.Map[String, Int]]) mustEqual java.util.Map.of()
      converter.convert("{}", classOf[java.util.Map[String, Int]]) mustEqual java.util.Map.of()
    }

    "return null if can't convert to Map" >> {
      val converter = createConverter(classOf[String], classOf[java.util.Map[String, Int]])
      val converted = converter.convert("foobar", classOf[java.util.Map[String, Int]])
      converted must beNull
    }
  }
}
