/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import java.time._
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}

import org.geotools.factory.GeoTools
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.converters.ScalaCollectionsConverterFactory.{ListToListConverter, MapToMapConverter}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.immutable.HashMap

@RunWith(classOf[JUnitRunner])
class ConverterFactoriesTest extends Specification {

  "ScalaCollectionsConverterFactory" should {

    val factory = new ScalaCollectionsConverterFactory

    "create a converter between" >> {

      "list interfaces" >> {
        "seq and list" >> {
          val converter = factory.createConverter(classOf[Seq[_]], classOf[java.util.List[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
        "list and sequence" >> {
          val converter = factory.createConverter(classOf[java.util.List[_]], classOf[Seq[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
      }

      "list subclasses" >> {
        "list and java list" >> {
          val converter = factory.createConverter(classOf[List[_]], classOf[java.util.List[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
        "java list and list" >> {
          val converter = factory.createConverter(classOf[java.util.List[_]], classOf[List[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
        "array list and sequence" >> {
          val converter = factory.createConverter(classOf[java.util.ArrayList[_]], classOf[Seq[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
        "sequence and array list" >> {
          val converter = factory.createConverter(classOf[Seq[_]], classOf[java.util.ArrayList[_]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[ListToListConverter]
        }
      }

      "map interfaces" >> {
        "map and java map" >> {
          val converter = factory.createConverter(classOf[Map[_, _]], classOf[java.util.Map[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
        "java map and map" >> {
          val converter = factory.createConverter(classOf[java.util.Map[_, _]], classOf[Map[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
      }

      "map subclasses" >> {
        "map and java hashmap" >> {
          val converter = factory.createConverter(classOf[Map[_, _]], classOf[java.util.HashMap[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
        "java hashmap and map" >> {
          val converter = factory.createConverter(classOf[java.util.HashMap[_, _]], classOf[Map[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
        "java map and hashmap" >> {
          val converter = factory.createConverter(classOf[java.util.Map[_, _]], classOf[HashMap[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
        "hashmap and java map" >> {
          val converter = factory.createConverter(classOf[HashMap[_, _]], classOf[java.util.Map[_, _]], null)
          converter must not(beNull)
          converter must beAnInstanceOf[MapToMapConverter]
        }
      }
    }

    "convert lists" >> {
      val converter = factory.createConverter(classOf[List[Int]], classOf[java.util.List[Int]], null)
      val converted = converter.convert(List(3, 2, 1), classOf[java.util.List[Int]])
      converted must beAnInstanceOf[java.util.List[Int]]
      converted.asInstanceOf[java.util.List[Int]].size mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(0) mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(1) mustEqual 2
      converted.asInstanceOf[java.util.List[Int]].get(2) mustEqual 1
    }

    "convert maps" >> {
      val converter = factory.createConverter(classOf[Map[String, Int]], classOf[java.util.Map[String, Int]], null)
      val converted = converter.convert(Map("one" -> 1, "two" -> 2), classOf[java.util.Map[String, Int]])
      converted must beAnInstanceOf[java.util.Map[String, Int]]
      converted.asInstanceOf[java.util.Map[String, Int]].size mustEqual 2
      converted.asInstanceOf[java.util.Map[String, Int]].get("one") mustEqual 1
      converted.asInstanceOf[java.util.Map[String, Int]].get("two") mustEqual 2
    }

    "return null for unhandled class types" >> {
      val converter = factory.createConverter(classOf[String], classOf[Int], null)
      converter must beNull
    }
  }

  "StringCollectionConverterFactory" should {

    val factory = new StringCollectionConverterFactory

    val hints = GeoTools.getDefaultHints
    hints.put(StringCollectionConverterFactory.ListTypeKey, classOf[Int])
    hints.put(StringCollectionConverterFactory.MapKeyTypeKey, classOf[String])
    hints.put(StringCollectionConverterFactory.MapValueTypeKey, classOf[Int])

    "create converters" >> {
      "string to list" >> {
        val converter = factory.createConverter(classOf[String], classOf[java.util.List[_]], hints)
        converter must not(beNull)
      }
      "string to map" >> {
        val converter = factory.createConverter(classOf[String], classOf[java.util.Map[_, _]], hints)
        converter must not(beNull)
      }
      "require list hints" >> {
        val converter = factory.createConverter(classOf[String], classOf[java.util.List[_]], GeoTools.getDefaultHints)
        converter must beNull
      }
      "require map hints" >> {
        val converter = factory.createConverter(classOf[String], classOf[java.util.Map[_, _]], GeoTools.getDefaultHints)
        converter must beNull
      }
      "return null for things it can't convert" >> {
        val converter = factory.createConverter(classOf[String], classOf[UUID], hints)
        converter must beNull
      }
    }

    "convert from java toString to List" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.List[Int]], hints)
      val list = new java.util.ArrayList[Int]
      list.add(3)
      list.add(2)
      list.add(1)
      val converted = converter.convert(list.toString, classOf[java.util.List[Int]])
      converted must beAnInstanceOf[java.util.List[Int]]
      converted.asInstanceOf[java.util.List[Int]].size mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(0) mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(1) mustEqual 2
      converted.asInstanceOf[java.util.List[Int]].get(2) mustEqual 1
    }

    "convert from geomesa string to List" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.List[Int]], hints)
      val converted = converter.convert("3,2,1", classOf[java.util.List[Int]])
      converted must beAnInstanceOf[java.util.List[Int]]
      converted.asInstanceOf[java.util.List[Int]].size mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(0) mustEqual 3
      converted.asInstanceOf[java.util.List[Int]].get(1) mustEqual 2
      converted.asInstanceOf[java.util.List[Int]].get(2) mustEqual 1
    }

    "return null if can't convert to List" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.List[Int]], hints)
      val converted = converter.convert("foobar", classOf[java.util.List[Int]])
      converted must beNull
    }

    "convert from java toString to Map" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.Map[String, Int]], hints)
      val map = new java.util.HashMap[String, Int]
      map.put("one", 1)
      map.put("two", 2)
      val converted = converter.convert(map.toString, classOf[java.util.Map[String, Int]])
      converted must beAnInstanceOf[java.util.Map[String, Int]]
      converted.asInstanceOf[java.util.Map[String, Int]].size mustEqual 2
      converted.asInstanceOf[java.util.Map[String, Int]].get("one") mustEqual 1
      converted.asInstanceOf[java.util.Map[String, Int]].get("two") mustEqual 2
    }

    "convert from geomesa string to Map" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.Map[String, Int]], hints)
      val converted = converter.convert("one->1,two->2", classOf[java.util.Map[String, Int]])
      converted must beAnInstanceOf[java.util.Map[String, Int]]
      converted.asInstanceOf[java.util.Map[String, Int]].size mustEqual 2
      converted.asInstanceOf[java.util.Map[String, Int]].get("one") mustEqual 1
      converted.asInstanceOf[java.util.Map[String, Int]].get("two") mustEqual 2
    }

    "return null if can't convert to Map" >> {
      val converter = factory.createConverter(classOf[String], classOf[java.util.Map[String, Int]], hints)
      val converted = converter.convert("foobar", classOf[java.util.Map[String, Int]])
      converted must beNull
    }
  }

  "Java Date Conversions" should {
    val factory = new JavaTimeConverterFactory

    val dStr = "2015-01-01T00:00:00.000Z"
    val date = Date.from(ZonedDateTime.parse(dStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant)

    "convert a range of ISO8601 strings to dates" >> {
      date must not(beNull)
      val converter = factory.createConverter(classOf[String], classOf[java.util.Date], null)
      converter.convert("2015-01-01T00:00:00.000Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00.000", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01", classOf[java.util.Date]) mustEqual date
    }

    "convert a date to a full ISO8601 string" >> {
      val converter = factory.createConverter(classOf[Date], classOf[String], null)
      converter.convert(date, classOf[String]) mustEqual "2015-01-01T00:00:00.000Z"
    }

    "convert java time classes to dates" >> {
      val times = Seq(
        Instant.ofEpochMilli(date.getTime),
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC))
      )
      foreach(times) { time =>
        factory.createConverter(time.getClass, classOf[Date], null).convert(time, classOf[Date]) mustEqual date
      }
    }
  }
}
