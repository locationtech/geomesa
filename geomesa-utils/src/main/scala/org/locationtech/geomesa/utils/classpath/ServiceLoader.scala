/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.classpath

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
  * Scala SPI loader helper
  */
object ServiceLoader extends LazyLogging {

  import scala.collection.JavaConverters._

  /**
   * Load all services
   *
   * @param loader optional classloader to use
   * @param ct classtag
   * @tparam T service type
   * @return list of services
   */
  def load[T](loader: Option[ClassLoader] = None)(implicit ct: ClassTag[T]): List[T] = {
    // check if the current class is a child of the context classloader
    // this fixes service loading in Accumulo's per-namespace classpaths
    val clas = ct.runtimeClass.asInstanceOf[Class[T]]
    val ldr = loader.getOrElse {
      def chain(cl: ClassLoader): Stream[ClassLoader] =
        if (cl == null) { Stream.empty } else { cl #:: chain(cl.getParent) }
      val ccl = Thread.currentThread().getContextClassLoader
      if (ccl == null || chain(clas.getClassLoader).contains(ccl)) {
        clas.getClassLoader
      } else {
        logger.warn(s"Using a context ClassLoader that does not contain the class to load (${clas.getName}): $ccl")
        ccl
      }
    }
    java.util.ServiceLoader.load(clas, ldr).asScala.toList
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
