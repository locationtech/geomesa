/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, HBaseAdmin}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.{Coprocessor, TableName}

/**
  * Reflection wrapper for method signature differences in the HBase API
  */
object HBaseVersions {

  /**
   * Create a new table
   *
   * @param admin admin connection to hbase
   * @param name table name
   * @param colFamilies column families
   * @param bloom bloom filter
   * @param compression compression
   * @param encoding data block encoding
   * @param coprocessor coprocessor class and optional jar path
   * @param splits initial table splits (empty for no splits)
   */
  def createTableAsync(
    admin: Admin,
    name: TableName,
    colFamilies: Seq[Array[Byte]],
    bloom: Option[BloomType],
    compression: Option[Algorithm],
    encoding: Option[DataBlockEncoding],
    inMemory: Option[Boolean],
    coprocessor: Option[(String, Option[Path])],
    splits: Seq[Array[Byte]]): Unit = {

    val descriptor = hTableDescriptorClass.getConstructor(classOf[TableName]).newInstance(name).asInstanceOf[AnyRef]

    colFamilies.foreach { k =>
      val column = hColumnDescriptorClass.getConstructor(classOf[Array[Byte]]).newInstance(k)
      bloom.foreach(_setBloomFilterType(column, _))
      compression.foreach(_setCompressionType(column, _))
      encoding.foreach(_setDataBlockEncoding(column, _))
      inMemory.foreach(_setInMemory(column, _))
      _addFamily(descriptor, column)
    }

    coprocessor.foreach { case (clas, path) =>
      _addCoprocessor(descriptor, clas, path.orNull, Coprocessor.PRIORITY_USER, null)
    }

    _createTableAsync(admin, descriptor, if (splits.isEmpty) { null } else { splits.toArray })
  }

  /**
   * Disable a table asynchronously
   *
   * @param admin admin hbase connection
   * @param table table to disable
   */
  def disableTableAsync(admin: Admin, table: TableName): Unit = _disableTableAsync(admin, table)

  /**
   * Checks whether HBase is available, and throws an exception if not
   *
   * @param conf HBase configuration
   */
  def checkAvailable(conf: Configuration): Unit = _available(conf)

  private lazy val hTableDescriptorClass = Class.forName("org.apache.hadoop.hbase.HTableDescriptor")

  private lazy val hColumnDescriptorClass = Class.forName("org.apache.hadoop.hbase.HColumnDescriptor")

  private lazy val _setBloomFilterType: (Any, BloomType) => Unit =
    findMethod(hColumnDescriptorClass, "setBloomFilterType", classOf[BloomType])

  private lazy val _setCompressionType: (Any, Algorithm) => Unit =
    findMethod(hColumnDescriptorClass, "setCompressionType", classOf[Algorithm])

  private lazy val _setDataBlockEncoding: (Any, DataBlockEncoding) => Unit =
    findMethod(hColumnDescriptorClass, "setDataBlockEncoding", classOf[DataBlockEncoding])

  private lazy val _setInMemory: (Any, Boolean) => Unit =
    findMethod(hColumnDescriptorClass, "setInMemory", classOf[Boolean])

  // HBase 1.3 signature: public HTableDescriptor addFamily(final HColumnDescriptor family)
  // CDH 5.12 signature: public void addFamily(final HColumnDescriptor family)
  private lazy val _addFamily: (Any, Any) => Unit =
    findMethod(hTableDescriptorClass, "addFamily", hColumnDescriptorClass)

  private lazy val _disableTableAsync: (Admin, TableName) => Unit =
    findMethod(classOf[Admin], "disableTableAsync", classOf[TableName])

  // HBase 1.3 signature:
  // public HTableDescriptor addCoprocessor(String className, Path jarFilePath, int priority, final Map&lt;String, String&gt; kvs)
  // CDH 5.12 signature:
  // public void addCoprocessor(String className, Path jarFilePath, int priority, final Map&lt;String, String&gt; kvs)
  private lazy val _addCoprocessor: (Any, String, Path, Int, java.util.Map[String, String]) => Unit = {
    val methods = hTableDescriptorClass.getMethods
    val method = methods.find(m => m.getName == "addCoprocessor" && m.getParameterCount == 4).getOrElse {
      throw new NoSuchMethodException("Couldn't find HTableDescriptor.addCoprocessor method")
    }
    val parameterTypes = method.getParameterTypes.asInstanceOf[Array[AnyRef]]
    val expected = Array[AnyRef](classOf[String], classOf[Path], classOf[Int], classOf[java.util.Map[String, String]])
    if (java.util.Arrays.equals(parameterTypes, expected)) {
      (descriptor, className, jarFilePath, priority, kvs) =>
        method.invoke(descriptor, className, jarFilePath, Int.box(priority), kvs)
    } else {
      throw new NoSuchMethodException(
        s"Couldn't find HTableDescriptor.addCoprocessor method with correct parameters: $method")
    }
  }

  private lazy val _createTableAsync: (Admin, AnyRef, Array[Array[Byte]]) => Unit = {
    val methods = classOf[Admin].getMethods
    val method = methods.find(m => m.getName == "createTableAsync" && m.getParameterCount == 2).getOrElse {
      throw new NoSuchMethodException("Couldn't find Admin.createTableAsync method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.lengthCompare(2) == 0
        && parameterTypes.head.isAssignableFrom(hTableDescriptorClass)
        && parameterTypes.last == classOf[Array[Array[Byte]]]) {
      (admin, descriptor, splits) => method.invoke(admin, descriptor, splits)
    } else {
      throw new NoSuchMethodException(s"Couldn't find Admin.createTableAsync method with correct parameters: $method")
    }
  }

  private lazy val _available: Configuration => Unit = {
    val names = Seq("available", "checkHBaseAvailable")
    val method = classOf[HBaseAdmin].getMethods.find(m => names.contains(m.getName)).getOrElse {
      throw new NoSuchMethodException("Couldn't find HBaseAdmin.available method")
    }
    if (method.getParameterCount != 1 || method.getParameterTypes.head != classOf[Configuration]) {
      throw new NoSuchMethodException(s"Couldn't find HBaseAdmin.available method with correct parameters: $method")
    }
    conf => method.invoke(null, conf)
  }

  private def findMethod(clas: Class[_], name: String, param: Class[_]): (Any, Any) => Unit = {
    val method = clas.getMethods.find(_.getName == name).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find ${clas.getSimpleName}.$name method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.length == 1 && parameterTypes.head == param) {
      (obj, p) => method.invoke(obj, p.asInstanceOf[AnyRef])
    } else {
      throw new NoSuchMethodException(
        s"Couldn't find ${clas.getSimpleName}.$name method with correct parameters: $method")
    }
  }
}
