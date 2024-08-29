/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.admin.{NewTableConfiguration, TimeType}
import org.apache.accumulo.core.client.{AccumuloClient, NamespaceExistsException, TableExistsException}
import org.apache.accumulo.core.conf.ClientProperty
import org.locationtech.geomesa.accumulo.AccumuloProperties.TableProperties.{TableCacheExpiry, TableCreationSync}
import org.locationtech.geomesa.accumulo.util.TableManager._
import org.locationtech.geomesa.index.DistributedLockTimeout
import org.locationtech.geomesa.index.utils.DistributedLocking
import org.locationtech.geomesa.index.utils.DistributedLocking.LocalLocking
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.geomesa.utils.zk.ZookeeperLocking

import java.io.Closeable
import java.util.concurrent.TimeUnit

/**
 * Manages table creation/deletion
 *
 * @param client accumulo client - note, this client must be cleaned up externally (as it's usually shared)
 */
class TableManager(client: AccumuloClient) {

  private val delegate: TableLock = TableSynchronization(TableCreationSync.get) match {
    case TableSynchronization.ZooKeeper => new TableLockZk(client)
    case TableSynchronization.Local     => new TableLockLocal(client)
    case TableSynchronization.None      => new TableLockNone(client)
  }

  /**
   * Create table if it does not exist.
   *
   * Note: to avoid having to lock and check namespaces, the namespace for the table must already exist. Generally
   * all tables for a data store share a namespace, and the namespace is created up front, so it will always exist.
   *
   * @param table table name
   * @param useLogicalTime use logical table time
   * @return true if table was created, false if table already exists
   */
  def ensureTableExists(table: String, useLogicalTime: Boolean = true): Boolean =
    delegate.ensureTableExists(table, if (useLogicalTime) { TimeType.LOGICAL } else { TimeType.MILLIS })

  /**
   * Create namespace if it does not exist
   *
   * @param namespace namespace
   */
  def ensureNamespaceExists(namespace: String): Unit = delegate.ensureNamespaceExists(namespace)

  /**
   * Delete a table
   *
   * @param table table to delete
   */
  def deleteTable(table: String): Unit = delegate.deleteTable(table)

  // note: shadows IndexAdapter.renameTable
  // noinspection ScalaUnusedSymbol
  def renameTable(from: String, to: String): Unit = delegate.renameTable(from, to)
}

object TableManager {

  /**
   * Types of synchronization available
   */
  object TableSynchronization extends Enumeration {

    val ZooKeeper, Local, None = Value

    def apply(value: String): TableSynchronization.Value = {
      Seq(ZooKeeper, Local, None).find(_.toString.equalsIgnoreCase(value)).getOrElse {
        throw new IllegalArgumentException(
          s"No matching value for '$value' - available sync types: ${Seq(ZooKeeper, Local, None).mkString(", ")}")
      }
    }
  }

  /**
   * No-op locking implementation of table utils
   *
   * @param client accumulo client
   */
  private class TableLockNone(client: AccumuloClient) extends TableLock(client) {
    override protected def acquireDistributedLock(key: String): Closeable = () => {}
    override protected def acquireDistributedLock(key: String, timeOut: Long): Option[Closeable] = Some(() => {})
  }

  /**
   * Local locking implementation of table utils
   *
   * @param client accumulo client
   */
  private class TableLockLocal(client: AccumuloClient) extends TableLock(client) with LocalLocking

  /**
   * Distributed zookeeper locking implementation of table utils
   *
   * @param client accumulo client
   */
  private class TableLockZk(client: AccumuloClient) extends TableLock(client) with ZookeeperLocking with LazyLogging {

    override protected val zookeepers: String =
      client.properties().getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey)

    override protected def onTableExists(table: String): Unit = {
      logger.warn(
        s"TableExistsException when creating '$table' - this indicates another " +
          "GeoMesa client is creating tables in an unsafe manner")
    }

    override protected def onNamespaceExists(namespace: String): Unit = {
      logger.warn(
        s"NamespaceExistsException when creating '$namespace' - this indicates another " +
          "GeoMesa client is creating tables in an unsafe manner")
    }
  }

  /**
   * Table utility class
   *
   * @param client accumulo client
   */
  private abstract class TableLock(client: AccumuloClient) extends DistributedLocking {

    private val timeoutMillis = DistributedLockTimeout.toDuration.map(_.toMillis).getOrElse {
      // note: this property has a valid default value so this exception should never be triggered
      throw new IllegalArgumentException(s"Couldn't convert '${DistributedLockTimeout.get}' to a duration")
    }

    // note: value is not used here, we're treating this as a set
    private val tableCache: Cache[String, java.lang.Boolean] =
      Caffeine.newBuilder().expireAfterWrite(TableCacheExpiry.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build()

    private val nsCache: Cache[String, java.lang.Boolean] =
      Caffeine.newBuilder().expireAfterWrite(TableCacheExpiry.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build()

    /**
     * Create the table if it doesn't exist
     *
     * @param table table name
     * @param timeType table time type
     * @return true if table was created, false if it already exists
     */
    def ensureTableExists(table: String, timeType: TimeType): Boolean = {
      var created = false
      tableCache.get(table, _ => {
        withLock(tablePath(table), timeoutMillis, {
          val tableOps = client.tableOperations()
          while (!tableOps.exists(table)) {
            try {
              tableOps.create(table, new NewTableConfiguration().setTimeType(timeType))
              created = true
            } catch {
              case _: TableExistsException => onTableExists(table)
            }
          }
        })
        java.lang.Boolean.FALSE
      })
      created
    }

    /**
     * Creates the namespace if it doesn't exist
     *
     * @param namespace namespace (or table name with namespace included)
     * @return true if namespace was created, false if it already existed
     */
    def ensureNamespaceExists(namespace: String): Unit = {
      val ns = namespace.takeWhile(_ != '.')
      nsCache.get(ns, _ => {
        withLock(nsPath(ns), timeoutMillis, {
          val nsOps = client.namespaceOperations
          while (!nsOps.exists(ns)) {
            try { nsOps.create(ns) } catch {
              case _: NamespaceExistsException => onNamespaceExists(ns)
            }
          }
        })
        java.lang.Boolean.FALSE
      })
    }

    /**
     * Rename a table
     *
     * @param from current name
     * @param to new name
     */
    def renameTable(from: String, to: String): Unit = {
      withLock(tablePath(from), timeoutMillis, {
        val tableOps = client.tableOperations()
        if (tableOps.exists(from)) {
          withLock(tablePath(to), timeoutMillis, {
            tableOps.rename(from, to)
            tableCache.put(to, java.lang.Boolean.FALSE)
          })
        }
        tableCache.invalidate(from)
      })
    }

    /**
     * Delete a table
     *
     * @param table table to delete
     */
    def deleteTable(table: String): Unit = {
      withLock(tablePath(table), timeoutMillis, {
        val tableOps = client.tableOperations()
        if (tableOps.exists(table)) {
          tableOps.delete(table)
        }
        tableCache.invalidate(table)
      })
    }

    // can happen sometimes with multiple threads but usually not a problem
    protected def onTableExists(table: String): Unit = {}
    // can happen sometimes with multiple threads but usually not a problem
    protected def onNamespaceExists(namespace: String): Unit = {}

    /**
     * ZK path for acquiring a table lock
     *
     * @param table table name
     * @return
     */
    private def tablePath(table: String): String =
      s"/org.locationtech.geomesa/table-locks/${StringSerialization.alphaNumericSafeString(table)}"

    /**
     * ZK path for acquiring a namespace lock
     *
     * @param namespace namespace
     * @return
     */
    private def nsPath(namespace: String): String =
      s"/org.locationtech.geomesa/ns-locks/${StringSerialization.alphaNumericSafeString(namespace)}"
  }
}
