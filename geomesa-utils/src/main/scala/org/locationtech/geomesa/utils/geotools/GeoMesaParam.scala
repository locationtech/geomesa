/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.{IOException, Serializable}
import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

class GeoMesaParam[T <: AnyRef](_key: String, // can't override final 'key' field from Param
                                description: String = "",
                                required: Boolean = false,
                                val default: T = null,
                                metadata: Map[String, Any] = null,
                                val deprecated: Seq[String] = Seq.empty,
                                val systemProperty: Option[(SystemProperty, (String) => T)] = None)
                               (implicit ct: ClassTag[T])
    extends Param(_key, ct.runtimeClass, description, required, default, Option(metadata).map(_.asJava).orNull)
      with LazyLogging {

  // ensure that sys property default is the same as param default, otherwise param default will not be used
  assert(systemProperty.forall { case (prop, convert) => prop.default == null || convert(prop.default) == default })

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
    if (params.containsKey(key)) {
      lookUp(params).asInstanceOf[T]
    } else if (deprecated.exists(params.containsKey)) {
      val oldKey = deprecated.find(params.containsKey).get
      deprecationWarning(oldKey)
      lookUp(Collections.singletonMap(key, params.get(oldKey))).asInstanceOf[T]
    } else if (required) {
      throw new IOException(s"Parameter $key is required: $description")
    } else {
      systemProperty.flatMap { case (prop, convert) => prop.option.map(convert) }.getOrElse(default)
    }
  }

  /**
    * Lookup for optional parameters. If a default or system property is defined, will be returned
    *
    * @param params parameter map
    * @return
    */
  def lookupOpt(params: java.util.Map[String, Serializable]): Option[T] = Option(lookup(params))

  /**
    * Logs a warning about deprecated parameter keys
    *
    * @param deprecated deprecated key found
    */
  def deprecationWarning(deprecated: String): Unit =
    logger.warn(s"Parameter '$deprecated' is deprecated, please use '$key' instead")


  @throws(classOf[Throwable])
  override def parse(text: String): AnyRef =
    if (ct.runtimeClass == classOf[Duration]) { Duration(text) } else { super.parse(text) }
}
