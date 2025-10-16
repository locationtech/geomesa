/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.util.{Converter, Converters}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.immutable.HashMap

@RunWith(classOf[JUnitRunner])
class ScalaCollectionConverterFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  private def createConverter(from: Class[_], to: Class[_]): Converter = {
    // note: we don't call Converters.getConverterFactories(from, to) as that tries to load jai-dependent classes
    val factory = Converters.getConverterFactories(null).asScala.find(_.isInstanceOf[ScalaCollectionsConverterFactory]).orNull
    factory must not(beNull)
    factory.createConverter(from, to, null)
  }

  "ScalaCollectionsConverterFactory" should {

    "create a converter between list interfaces" >> {
      "seq and list" >> {
        val converter = createConverter(classOf[Seq[_]], classOf[java.util.List[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.SeqToJava)
      }
      "list and mutable sequence" >> {
        val converter = createConverter(classOf[java.util.List[_]], classOf[scala.collection.mutable.Seq[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToMutableSeq)
      }
      "list and immutable sequence" >> {
        val converter = createConverter(classOf[java.util.List[_]], classOf[scala.collection.immutable.Seq[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableSeq)
      }
    }

    "create a converter between list subclasses" >> {
      "list and java list" >> {
        val converter = createConverter(classOf[List[_]], classOf[java.util.List[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.SeqToJava)
      }
      "java list and immutable list" >> {
        val converter = createConverter(classOf[java.util.List[_]], classOf[scala.collection.immutable.List[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableSeq)
      }
      "array list and immutable sequence" >> {
        val converter = createConverter(classOf[java.util.ArrayList[_]], classOf[scala.collection.immutable.Seq[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableSeq)
      }
      "array list and mutable sequence" >> {
        val converter = createConverter(classOf[java.util.ArrayList[_]], classOf[scala.collection.mutable.Seq[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToMutableSeq)
      }
      "sequence and array list" >> {
        val converter = createConverter(classOf[Seq[_]], classOf[java.util.ArrayList[_]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.SeqToJava)
      }
    }

    "create a converter between map interfaces" >> {
      "map and java map" >> {
        val converter = createConverter(classOf[Map[_, _]], classOf[java.util.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.MapToJava)
      }
      "java map and immutable map" >> {
        val converter = createConverter(classOf[java.util.Map[_, _]], classOf[scala.collection.immutable.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableMap)
      }
      "java map and mutable map" >> {
        val converter = createConverter(classOf[java.util.Map[_, _]], classOf[scala.collection.mutable.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToMutableMap)
      }
    }

    "create a converter between map subclasses" >> {
      "map and java hashmap" >> {
        val converter = createConverter(classOf[Map[_, _]], classOf[java.util.HashMap[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.MapToJava)
      }
      "java hashmap and immutable map" >> {
        val converter = createConverter(classOf[java.util.HashMap[_, _]], classOf[scala.collection.immutable.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableMap)
      }
      "java hashmap and mutable map" >> {
        val converter = createConverter(classOf[java.util.HashMap[_, _]], classOf[scala.collection.mutable.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToMutableMap)
      }
      "java map and immutable hashmap" >> {
        val converter = createConverter(classOf[java.util.Map[_, _]], classOf[scala.collection.immutable.HashMap[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToImmutableMap)
      }
      "java map and mutable hashmap" >> {
        val converter = createConverter(classOf[java.util.Map[_, _]], classOf[scala.collection.mutable.HashMap[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.JavaToMutableMap)
      }
      "hashmap and java map" >> {
        val converter = createConverter(classOf[HashMap[_, _]], classOf[java.util.Map[_, _]])
        converter must beTheSameAs(ScalaCollectionsConverterFactory.MapToJava)
      }
    }

    "convert lists" >> {
      val converter = createConverter(classOf[List[Int]], classOf[java.util.List[Int]])
      val converted = converter.convert(List(3, 2, 1), classOf[java.util.List[Int]])
      converted must not(beNull)
      converted.size mustEqual 3
      converted.get(0) mustEqual 3
      converted.get(1) mustEqual 2
      converted.get(2) mustEqual 1
    }

    "convert maps" >> {
      val converter = createConverter(classOf[Map[String, Int]], classOf[java.util.Map[String, Int]])
      val converted = converter.convert(Map("one" -> 1, "two" -> 2), classOf[java.util.Map[String, Int]])
      converted must not(beNull)
      converted.size mustEqual 2
      converted.get("one") mustEqual 1
      converted.get("two") mustEqual 2
    }

    "return null for unhandled class types" >> {
      val converter = createConverter(classOf[String], classOf[Int])
      converter must beNull
    }
  }
}
