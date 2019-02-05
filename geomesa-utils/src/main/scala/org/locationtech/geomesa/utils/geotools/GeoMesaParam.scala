/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.{IOException, Serializable, StringReader, StringWriter}
import java.util.{Collections, Properties}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.Parameter
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam._
import org.locationtech.geomesa.utils.text.DurationParsing

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Data store parameter, used for configuring data stores in DataStoreFinder
  *
  * @param _key key used to look up values
  * @param desc readable description of the parameter
  * @param optional optional, or required
  * @param default default value, if any
  * @param password is the parameter a password
  * @param largeText should the parameter use a text area instead of a text box in a GUI
  * @param extension filter for file upload extensions
  * @param deprecatedKeys deprecated keys for this parameter
  * @param deprecatedParams deprecated params replaced by this parameter, but that require conversion
  * @param systemProperty system property used as a fallback lookup
  * @param enumerations list of values used to restrain the user input
  */
class GeoMesaParam[T <: AnyRef](_key: String, // can't override final 'key' field from Param
                                desc: String = "", // can't override final 'description' field from Param
                                optional: Boolean = true, // can't override final 'required' field from Param
                                val default: T = null,
                                val password: Boolean = false,
                                val largeText: Boolean = false,
                                val extension: String = null,
                                val deprecatedKeys: Seq[String] = Seq.empty,
                                val deprecatedParams: Seq[DeprecatedParam[T]] = Seq.empty,
                                val systemProperty: Option[SystemPropertyParam[T]] = None,
                                val enumerations: Seq[T] = Seq.empty)
                               (implicit ct: ClassTag[T])
    extends Param(_key, binding(ct), desc, !optional, sample(default), metadata(password, largeText, extension, enumerations))
      with LazyLogging {

  private val deprecated = deprecatedKeys ++ deprecatedParams.map(_.key)

  private val toTypedValue: AnyRef => T = {
    if (ct.runtimeClass == classOf[Duration]) {
      v => GeoMesaParam.parseDuration(v.asInstanceOf[String]).asInstanceOf[T]
    } else if (ct.runtimeClass == classOf[Properties]) {
      v => GeoMesaParam.parseProperties(v.asInstanceOf[String]).asInstanceOf[T]
    } else {
      v => v.asInstanceOf[T]
    }
  }

  private val fromTypedValue: T => AnyRef = {
    if (ct.runtimeClass == classOf[Duration]) {
      v => GeoMesaParam.printDuration(v.asInstanceOf[Duration])
    } else if (ct.runtimeClass == classOf[Properties]) {
      v => GeoMesaParam.printProperties(v.asInstanceOf[Properties])
    } else {
      v => v
    }
  }

  // ensure that sys property default is the same as param default, otherwise param default will not be used
  assert(systemProperty.forall(p => p.prop.default == null || toTypedValue(parse(p.prop.default)) == default))

  /**
    * Checks that the parameter is contained in the map, but does not do type conversion
    *
    * @param params parameter map
    * @return
    */
  def exists(params: java.util.Map[String, _ <: Serializable]): Boolean =
    params.get(key) != null || deprecated.exists(params.get(_) != null)

  /**
    * Returns the typed value from the map. Priority is:
    *
    *   1. primary key from the map
    *   2. deprecated keys from the map
    *   3. system property, if defined
    *   4. default value, if defined
    *   5. null
    *
    * Required parameters must be contained in the map, they will not fall back to
    * default values or system properties
    *
    * @param params parameter map
    * @return
    */
  def lookup(params: java.util.Map[String, _ <: Serializable]): T = {
    val value = if (params.containsKey(key)) {
      lookUp(params)
    } else if (deprecated.exists(params.containsKey)) {
      val oldKey = deprecated.find(params.containsKey).get
      deprecationWarning(oldKey)
      if (deprecatedKeys.contains(oldKey)) {
        lookUp(Collections.singletonMap(key, params.get(oldKey)))
      } else {
        fromTypedValue(deprecatedParams.dropWhile(_.key != oldKey).head.lookup(params, required))
      }
    } else if (required) {
      throw new IOException(s"Parameter $key is required: $description")
    } else {
      systemProperty.flatMap(_.option) match {
        case Some(v) => fromTypedValue(v)
        case None    => null
      }
    }
    if (value == null) { default } else {
      try { toTypedValue(value) } catch {
        case NonFatal(e) => throw new IOException(s"Invalid property for parameter '$key': $value", e)
      }
    }
  }

  /**
    * Lookup for optional parameters. If a default or system property is defined, will be returned
    *
    * @param params parameter map
    * @return
    */
  def lookupOpt(params: java.util.Map[String, _ <: Serializable]): Option[T] = Option(lookup(params))

  /**
    * Logs a warning about deprecated parameter keys
    *
    * @param deprecated deprecated key found
    */
  def deprecationWarning(deprecated: String): Unit =
    logger.warn(s"Parameter '$deprecated' is deprecated, please use '$key' instead")

  override def text(value: AnyRef): String = super.text(fromTypedValue(value.asInstanceOf[T]))
}

object GeoMesaParam {

  import scala.collection.JavaConverters._

  trait DeprecatedParam[T <: AnyRef] {
    def key: String
    def lookup(params: java.util.Map[String, _ <: Serializable], required: Boolean): T
  }

  case class ConvertedParam[T <: AnyRef, U <: AnyRef](key: String, convert: U => T)(implicit ct: ClassTag[U])
      extends DeprecatedParam[T] {
    override def lookup(params: java.util.Map[String, _ <: Serializable], required: Boolean): T = {
      val res = Option(new Param(key, ct.runtimeClass, "", required).lookUp(params).asInstanceOf[U]).map(convert)
      res.asInstanceOf[Option[AnyRef]].orNull.asInstanceOf[T] // scala compiler forces these casts...
    }
  }

  trait SystemPropertyParam[T] {
    def prop: SystemProperty
    def option: Option[T]
  }

  case class SystemPropertyStringParam(prop: SystemProperty) extends SystemPropertyParam[String] {
    override def option: Option[String] = prop.option
  }

  case class SystemPropertyBooleanParam(prop: SystemProperty) extends SystemPropertyParam[java.lang.Boolean] {
    override def option: Option[java.lang.Boolean] = prop.toBoolean.map(Boolean.box)
  }

  case class SystemPropertyIntegerParam(prop: SystemProperty) extends SystemPropertyParam[Integer] {
    override def option: Option[Integer] = prop.option.map(Integer.valueOf)
  }

  case class SystemPropertyDurationParam(prop: SystemProperty) extends SystemPropertyParam[Duration] {
    override def option: Option[Duration] = prop.toDuration
  }

  def binding[T <: AnyRef](ct: ClassTag[T]): Class[_] = ct.runtimeClass match {
    case c if c == classOf[Duration] | c == classOf[Properties] => classOf[String]
    case c => c
  }

  def sample[T <: AnyRef](value: T): AnyRef = value match {
    case null => null
    case v: Duration => printDuration(v)
    case v: Properties => printProperties(v)
    case v => v
  }

  def metadata(password: Boolean,
               largeText: Boolean,
               extension: String,
               enumerations: Seq[AnyRef]): java.util.Map[String, AnyRef] = {
    // presumably we wouldn't have any combinations of these...
    if (password) {
      java.util.Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE)
    } else if (largeText) {
      java.util.Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE)
    } else if (extension != null) {
      java.util.Collections.singletonMap(Parameter.EXT, extension)
    } else if (enumerations.nonEmpty) {
      // convert to a mutable java list, as geoserver tries to sort it in place
      val enums = new java.util.ArrayList[AnyRef](enumerations.length)
      enumerations.foreach(enums.add)
      java.util.Collections.singletonMap(Parameter.OPTIONS, enums)
    } else {
      null
    }
  }

  private def parseDuration(text: String): Duration = DurationParsing.caseInsensitive(text)

  private def printDuration(duration: Duration): String =
    if (duration == Duration.Inf) { "Inf" } else { duration.toString }

  private def parseProperties(text: String): Properties = {
    val props = new Properties
    props.load(new StringReader(text))
    props
  }

  private def printProperties(properties: Properties): String = {
    val out = new StringWriter()
    properties.store(out, null)
    out.toString
  }
}
