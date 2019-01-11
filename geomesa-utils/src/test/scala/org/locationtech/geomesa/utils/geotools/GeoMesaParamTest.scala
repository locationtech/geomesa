/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.utils.geotools

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{SystemPropertyBooleanParam, SystemPropertyDurationParam, SystemPropertyIntegerParam, SystemPropertyStringParam}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class GeoMesaParamTest extends Specification with LazyLogging {

  import scala.collection.JavaConversions._

  "GeoMesaParam" should {
    "look up values" in {
      new GeoMesaParam[String]("foo").lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[Integer]("foo").lookup(Map("foo" -> Int.box(1))) mustEqual 1
      new GeoMesaParam[java.lang.Boolean]("foo").lookup(Map("foo" -> Boolean.box(true))) mustEqual true
    }
    "look up null values" in {
      new GeoMesaParam[String]("foo").lookup(Map.empty[String, String]) must beNull
      new GeoMesaParam[Integer]("foo").lookup(Map.empty[String, String]) must beNull
      new GeoMesaParam[java.lang.Boolean]("foo").lookup(Map.empty[String, String]) must beNull
    }
    "look up optional values" in {
      new GeoMesaParam[String]("foo").lookupOpt(Map("foo" -> "bar")) must beSome("bar")
      new GeoMesaParam[Integer]("foo").lookupOpt(Map("foo" -> Int.box(1))) must beSome(Int.box(1))
      new GeoMesaParam[java.lang.Boolean]("foo").lookupOpt(Map("foo" -> Boolean.box(true))) must beSome(Boolean.box(true))
    }
    "look up missing optional values" in {
      new GeoMesaParam[String]("foo").lookupOpt(Map.empty[String, String]) must beNone
      new GeoMesaParam[Integer]("foo").lookupOpt(Map.empty[String, String]) must beNone
      new GeoMesaParam[java.lang.Boolean]("foo").lookupOpt(Map.empty[String, String]) must beNone
    }
    "look up values with defaults" in {
      new GeoMesaParam[String]("foo", default = "baz").lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[Integer]("foo", default = 2).lookup(Map("foo" -> Int.box(1))) mustEqual 1
      new GeoMesaParam[java.lang.Boolean]("foo", default = false).lookup(Map("foo" -> Boolean.box(true))) mustEqual true
    }
    "look up default values" in {
      new GeoMesaParam[String]("foo", default = "bar").lookup(Map.empty[String, String]) mustEqual "bar"
      new GeoMesaParam[Integer]("foo", default = 2).lookup(Map.empty[String, String]) mustEqual 2
      new GeoMesaParam[java.lang.Boolean]("foo", default = true).lookup(Map.empty[String, String]) mustEqual true
    }
    "look up required values" in {
      new GeoMesaParam[String]("foo", optional = false).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[Integer]("foo", optional = false).lookup(Map("foo" -> Int.box(1))) mustEqual 1
      new GeoMesaParam[java.lang.Boolean]("foo", optional = false).lookup(Map("foo" -> Boolean.box(true))) mustEqual true
    }
    "throw exception for missing required values" in {
      new GeoMesaParam[String]("foo", optional = false).lookup(Map.empty[String, String]) must throwAn[IOException]
      new GeoMesaParam[Integer]("foo", optional = false).lookup(Map.empty[String, String]) must throwAn[IOException]
      new GeoMesaParam[java.lang.Boolean]("foo", optional = false).lookup(Map.empty[String, String]) must throwAn[IOException]
    }
    "throw exception for invalid type conversions" in {
      new GeoMesaParam[Integer]("foo").lookup(Map("foo" -> "bar")) must throwAn[IOException]
    }
    "lookup deprecated values" in {
      new GeoMesaParam[String]("foo", deprecatedKeys = Seq("notfoo")).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", deprecatedKeys = Seq("notfoo")).lookup(Map("notfoo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", optional = false, deprecatedKeys = Seq("notfoo")).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", optional = false, deprecatedKeys = Seq("notfoo")).lookup(Map("notfoo" -> "bar")) mustEqual "bar"
    }
    "look up system properties" in {
      val prop = SystemProperty("params.foo.bar")
      prop.threadLocalValue.set("baz")
      new GeoMesaParam[String]("foo", systemProperty = Some(SystemPropertyStringParam(prop))).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", systemProperty = Some(SystemPropertyStringParam(prop))).lookup(Map.empty[String, String]) mustEqual "baz"
      prop.threadLocalValue.set("2")
      new GeoMesaParam[Integer]("foo", systemProperty = Some(SystemPropertyIntegerParam(prop))).lookup(Map("foo" -> Int.box(1))) mustEqual 1
      new GeoMesaParam[Integer]("foo", systemProperty = Some(SystemPropertyIntegerParam(prop))).lookup(Map.empty[String, String]) mustEqual 2
      prop.threadLocalValue.set("true")
      new GeoMesaParam[java.lang.Boolean]("foo", systemProperty = Some(SystemPropertyBooleanParam(prop))).lookup(Map("foo" -> Boolean.box(false))) mustEqual false
      new GeoMesaParam[java.lang.Boolean]("foo", systemProperty = Some(SystemPropertyBooleanParam(prop))).lookup(Map.empty[String, String]) mustEqual true
    }
    "not accept system properties for required parameters" in {
      val prop = SystemProperty("params.foo.bar")
      prop.threadLocalValue.set("baz")
      new GeoMesaParam[String]("foo", optional = false, systemProperty = Some(SystemPropertyStringParam(prop))).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", optional = false, systemProperty = Some(SystemPropertyStringParam(prop))).lookup(Map.empty[String, String]) must throwAn[IOException]
      prop.threadLocalValue.set("2")
      new GeoMesaParam[Integer]("foo", optional = false, systemProperty = Some(SystemPropertyIntegerParam(prop))).lookup(Map("foo" -> Int.box(1))) mustEqual 1
      new GeoMesaParam[Integer]("foo", optional = false, systemProperty = Some(SystemPropertyIntegerParam(prop))).lookup(Map.empty[String, String]) must throwAn[IOException]
      prop.threadLocalValue.set("true")
      new GeoMesaParam[java.lang.Boolean]("foo", optional = false, systemProperty = Some(SystemPropertyBooleanParam(prop))).lookup(Map("foo" -> Boolean.box(false))) mustEqual false
      new GeoMesaParam[java.lang.Boolean]("foo", optional = false, systemProperty = Some(SystemPropertyBooleanParam(prop))).lookup(Map.empty[String, String]) must throwAn[IOException]
    }
    "prioritize system properties over default values" in {
      val prop = SystemProperty("params.foo.bar")
      val sysParam = SystemPropertyStringParam(prop)
      prop.threadLocalValue.set("baz")
      new GeoMesaParam[String]("foo", default = "wuz", systemProperty = Some(sysParam)).lookup(Map("foo" -> "bar")) mustEqual "bar"
      new GeoMesaParam[String]("foo", default = "wuz", systemProperty = Some(sysParam)).lookup(Map.empty[String, String]) mustEqual "baz"
      prop.threadLocalValue.remove()
      new GeoMesaParam[String]("foo", default = "wuz", systemProperty = Some(sysParam)).lookup(Map.empty[String, String]) mustEqual "wuz"
    }
    "require system properties to have a common default" in {
      new GeoMesaParam[String]("foo", systemProperty = Some(SystemPropertyStringParam(SystemProperty("params.foo.bar", "baz")))) must throwAn[AssertionError]
      new GeoMesaParam[String]("foo", default = "bar", systemProperty = Some(SystemPropertyStringParam(SystemProperty("params.foo.bar", "baz")))) must throwAn[AssertionError]
      new GeoMesaParam[String]("foo", default = "bar", systemProperty = Some(SystemPropertyStringParam(SystemProperty("params.foo.bar", "bar")))) must not(throwAn[AssertionError])
      new GeoMesaParam[Integer]("foo", systemProperty = Some(SystemPropertyIntegerParam(SystemProperty("params.foo.bar", "2")))) must throwAn[AssertionError]
      new GeoMesaParam[Integer]("foo", default = 1, systemProperty = Some(SystemPropertyIntegerParam(SystemProperty("params.foo.bar", "2")))) must throwAn[AssertionError]
      new GeoMesaParam[Integer]("foo", default = 1, systemProperty = Some(SystemPropertyIntegerParam(SystemProperty("params.foo.bar", "1")))) must not(throwAn[AssertionError])
    }
    "lookup durations" in {
      new GeoMesaParam[Duration]("foo").lookup(Map("foo" -> "10s")) mustEqual Duration("10s")
      new GeoMesaParam[Duration]("foo").lookup(Map("foo" -> "10S")) mustEqual Duration("10s")
      new GeoMesaParam[Duration]("foo").lookup(Map("foo" -> "Inf")) mustEqual Duration.Inf
      new GeoMesaParam[Duration]("foo").lookup(Map("foo" -> "inf")) mustEqual Duration.Inf
      new GeoMesaParam[Duration]("foo").lookup(Map("foo" -> "bar")) must throwAn[IOException]
    }
    "lookup durations with defaults" in {
      new GeoMesaParam[Duration]("foo", default = Duration("10s")).lookup(Map("foo" -> "10s")) mustEqual Duration("10s")
      new GeoMesaParam[Duration]("foo", default = Duration("10s")).lookup(Map.empty[String, String]) mustEqual Duration("10s")
    }
    "lookup durations with defaults and system properties" in {
      new GeoMesaParam[Duration]("foo", default = Duration("10s"), systemProperty = Some(SystemPropertyDurationParam(SystemProperty("params.foo", "10s")))).lookup(Map("foo" -> "10s")) mustEqual Duration("10s")
      new GeoMesaParam[Duration]("foo", default = Duration("10s"), systemProperty = Some(SystemPropertyDurationParam(SystemProperty("params.foo", "10s")))).lookup(Map.empty[String, String]) mustEqual Duration("10s")
    }
  }
}
