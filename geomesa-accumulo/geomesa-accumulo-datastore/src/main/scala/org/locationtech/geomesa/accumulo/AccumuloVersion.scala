/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.{Connector, TableExistsException}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.utils.conf.SemanticVersion

import scala.reflect.ClassTag
import scala.util.{Success, Try}

object AccumuloVersion extends Enumeration with LazyLogging {

  type AccumuloVersion = Value
  val V15, V16, V17, V18, V19 = Value

  lazy val accumuloVersion: AccumuloVersion = Try(SemanticVersion(Constants.VERSION, lenient = true)) match {
    case Success(v) if v.major == 1 && v.minor == 9 => V19
    case Success(v) if v.major == 1 && v.minor == 8 => V18
    case Success(v) if v.major == 1 && v.minor == 7 => V17
    case Success(v) if v.major == 1 && v.minor == 6 => V16
    case Success(v) if v.major == 1 && v.minor == 5 => V15
    case Success(v) if (v.major == 1 && v.minor > 9) || v.major > 1 =>
      logger.warn(s"Found unsupported version ${Constants.VERSION}; using 1.9 compatibility mode"); V19

    case _ =>
      throw new IllegalStateException(s"GeoMesa does not currently support Accumulo version ${Constants.VERSION}")
  }

  lazy val AccumuloMetadataTableName: String = getMetadataTable
  lazy val AccumuloMetadataCF: Text = getMetadataColumnFamily
  lazy val EmptyAuths: Authorizations = getEmptyAuths

  def getMetadataTable: String = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "METADATA_TABLE_NAME")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
    }
  }

  def getMetadataColumnFamily: Text = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "METADATA_DATAFILE_COLUMN_FAMILY")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
    }
  }

  def getEmptyAuths: Authorizations = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "NO_AUTHS")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
    }
  }

  /**
    * Creates the table if it doesn't exist
    *
    * @param connector connector
    * @param table table name
    * @param logical use logical time?
    * @return true if table was created, false if it already existed
    */
  def createTableIfNeeded(connector: Connector, table: String, logical: Boolean = true): Boolean = {
    val tableOps = connector.tableOperations()
    if (tableOps.exists(table)) { false } else {
      val dot = table.indexOf('.')
      if (dot > 0) {
        createNamespaceIfNeeded(connector, table.substring(0, dot))
      }
      try { tableOps.create(table, true, if (logical) { TimeType.LOGICAL } else { TimeType.MILLIS }); true } catch {
        // this can happen with multiple threads but shouldn't cause any issues
        case _: TableExistsException => false
      }
    }
  }

  /**
    * Creates the namespace if it doesn't exist
    *
    * @param connector connector
    * @param namespace namespace
    * @return true if namespace was created, false if it already existed
    */
  def createNamespaceIfNeeded(connector: Connector, namespace: String): Boolean = {
    require(accumuloVersion != V15, s"Table namespaces are not supported in Accumulo 1.5 - for namespace '$namespace'")
    val nsOps = classOf[Connector].getMethod("namespaceOperations").invoke(connector)
    if (nameSpaceExists(nsOps, nsOps.getClass, namespace)) { false } else {
      val createMethod = nsOps.getClass.getMethod("create", classOf[String])
      createMethod.setAccessible(true)
      try { createMethod.invoke(nsOps, namespace); true } catch {
        // this can happen with multiple threads but shouldn't cause any issue
        case e: Exception if e.getClass.getSimpleName == "NamespaceExistsException" => false
      }
    }
  }

  @deprecated("Replaced with createTableIfNeeded")
  def ensureTableExists(connector: Connector, table: String, logical: Boolean = true): Unit =
    createTableIfNeeded(connector, table, logical)

  @deprecated("Replaced with createNamespaceIfNeeded")
  def ensureNamespaceExists(connector: Connector, namespace: String): Unit = createNamespaceIfNeeded(connector, namespace)

  private def nameSpaceExists(nsOps: AnyRef, nsOpsClass: Class[_], ns: String): Boolean = {
    val existsMethod = nsOpsClass.getMethod("exists", classOf[String])
    existsMethod.setAccessible(true)
    existsMethod.invoke(nsOps, ns).asInstanceOf[Boolean]
  }

  private def getTypeFromClass[T: ClassTag](className: String, field: String): T = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(null).asInstanceOf[T]
  }
}