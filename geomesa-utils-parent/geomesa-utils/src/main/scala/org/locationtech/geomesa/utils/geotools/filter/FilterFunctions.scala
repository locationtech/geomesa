/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.filter

import org.opengis.parameter.Parameter

import scala.reflect.ClassTag

object FilterFunctions {

  import scala.collection.JavaConverters._

  /**
   * Create a function parameter
   *
   * @param name param name
   * @param required required
   * @param max max occurs
   * @param sample example value
   * @param metadata metadata
   * @tparam T param type
   * @return
   */
  def parameter[T <: AnyRef : ClassTag](
      name: String,
      required: Boolean = false,
      max: Int = 1,
      sample: T = null,
      metadata: Map[String, AnyRef] = Map.empty): Parameter[T] = {
    val clas = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val min = if (required) { 1 } else { 0 }
    val meta = if (metadata.isEmpty) { null } else { metadata.asJava }
    new org.geotools.data.Parameter[T](name, clas, null, null, required, min, max, sample, meta)
  }
}
