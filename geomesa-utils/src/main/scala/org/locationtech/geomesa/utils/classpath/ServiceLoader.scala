/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.classpath

import scala.reflect.ClassTag

/**
  * Scala SPI loader helper
  */
object ServiceLoader {

  import scala.collection.JavaConverters._

  /**
    * Load all services
    *
    * @param ct classtag
    * @tparam T service type
    * @return list of services
    */
  def load[T](loader: Option[ClassLoader] = None)(implicit ct: ClassTag[T]): List[T] = {
    val result = loader match {
      case None      => java.util.ServiceLoader.load(ct.runtimeClass.asInstanceOf[Class[T]])
      case Some(ldr) => java.util.ServiceLoader.load(ct.runtimeClass.asInstanceOf[Class[T]], ldr)
    }
    result.asScala.toList
  }

  /**
    * Load a service. If there is not exactly 1 implementation found, throws an exception
    *
    * @param ct classtag
    * @tparam T service type
    * @throws IllegalStateException if there is not exactly 1 service found
    * @return service
    */
  @throws[IllegalStateException]
  def loadExactlyOne[T](loader: Option[ClassLoader] = None)(implicit ct: ClassTag[T]): T = {
    val all = load[T](loader)
    if (all.lengthCompare(1) != 0) {
      throw new IllegalStateException(s"Expected 1 instance of ${ct.runtimeClass.getName} but found ${all.length}")
    }
    all.head
  }

  /**
    * Load a service. If there is not exactly 0 or 1 implementations found, throws an exception
    *
    * @param ct classtag
    * @tparam T service type
    * @throws IllegalStateException if there is not exactly 0 or 1 service found
    * @return service, if found
    */
  @throws[IllegalStateException]
  def loadAtMostOne[T](loader: Option[ClassLoader] = None)(implicit ct: ClassTag[T]): Option[T] = {
    val all = load[T](loader)
    if (all.lengthCompare(1) > 0) {
      throw new IllegalStateException(s"Expected 0 or 1 instances of ${ct.runtimeClass.getName} but found ${all.length}")
    }
    all.headOption
  }
}
