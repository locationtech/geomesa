/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{Coprocessor, HColumnDescriptor, HTableDescriptor}

/**
  * Reflection wrapper for method signature differences in the HBase API
  */
object HBaseVersions {

  private val hTableDescriptorMethods = classOf[HTableDescriptor].getDeclaredMethods

  /**
    * HBase 1.3 signature: public HTableDescriptor addFamily(final HColumnDescriptor family)
    * CDH 5.12 signature: public void addFamily(final HColumnDescriptor family)
    *
    * @see `HTableDescriptor.addFamily`
    *
    * @param descriptor table descriptor
    * @param family column family descriptor
    */
  def addFamily(descriptor: HTableDescriptor, family: HColumnDescriptor): Unit = _addFamily(descriptor, family)

  /**
    * HBase 1.3 signature: public HTableDescriptor addCoprocessor(String className, Path jarFilePath,
                             int priority, final Map&lt;String, String&gt; kvs)
    * CDH 5.12 signature: public void addCoprocessor(String className, Path jarFilePath,
                             int priority, final Map&lt;String, String&gt; kvs)
    *
    * @see `HTableDescriptor.addCoprocessor`
    *
    * @param descriptor table descriptor
    * @param className class name of coprocessor
    * @param jarFilePath optional path to jar file containing coprocessor
    * @param priority optional priority for the coprocessor
    * @param kvs optional configuration for the coprocessor
    */
  def addCoprocessor(descriptor: HTableDescriptor,
                     className: String,
                     jarFilePath: Option[Path] = None,
                     priority: Int = Coprocessor.PRIORITY_USER,
                     kvs: java.util.Map[String, String] = null): Unit =
    _addCoprocessor(descriptor, className, jarFilePath.orNull, priority, kvs)

  private val _addFamily: (HTableDescriptor, HColumnDescriptor) => Unit = {
    val method = hTableDescriptorMethods.find(_.getName == "addFamily").getOrElse {
      throw new NoSuchMethodException("Couldn't find HTableDescriptor.addFamily method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.length == 1 && parameterTypes.head == classOf[HColumnDescriptor]) {
      (descriptor, family) => method.invoke(descriptor, family)
    } else {
      throw new NoSuchMethodException(s"Couldn't find HTableDescriptor.addFamily method with correct parameters: $method")
    }
  }

  private val _addCoprocessor: (HTableDescriptor, String, Path, Int, java.util.Map[String, String]) => Unit = {
    val method = hTableDescriptorMethods.find(m => m.getName == "addCoprocessor" && m.getParameterCount == 4).getOrElse {
      throw new NoSuchMethodException("Couldn't find HTableDescriptor.addCoprocessor method")
    }
    val parameterTypes = method.getParameterTypes.asInstanceOf[Array[AnyRef]]
    val expected = Array[AnyRef](classOf[String], classOf[Path], classOf[Int], classOf[java.util.Map[String, String]])
    if (java.util.Arrays.equals(parameterTypes, expected)) {
      (descriptor, className, jarFilePath, priority, kvs) =>
        method.invoke(descriptor, className, jarFilePath, Int.box(priority), kvs)
    } else {
      throw new NoSuchMethodException(s"Couldn't find HTableDescriptor.addCoprocessor method with correct parameters: $method")
    }
  }

}
