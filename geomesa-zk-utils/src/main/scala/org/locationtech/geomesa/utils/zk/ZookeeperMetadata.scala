/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.zk

import java.nio.charset.StandardCharsets

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.locationtech.geomesa.index.metadata.{CachedLazyBinaryMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator

class ZookeeperMetadata[T](val namespace: String, val zookeepers: String, val serializer: MetadataSerializer[T])
    extends CachedLazyBinaryMetadata[T] {

  import org.locationtech.geomesa.utils.zk.ZookeeperMetadata.Root

  private val client = CuratorFrameworkFactory.builder()
      .namespace(namespace)
      .connectString(zookeepers)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
  client.start()

  override protected def checkIfTableExists: Boolean = true

  override protected def createTable(): Unit = {}

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (row, value) =>
      val path = toPath(row)
      if (client.checkExists().forPath(path) == null) {
        client.create().creatingParentsIfNeeded().forPath(path)
      }
      client.setData().forPath(path, value)
    }
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    rows.foreach { row =>
      val path = toPath(row)
      if (client.checkExists().forPath(path) != null) {
        client.delete().deletingChildrenIfNeeded().forPath(path)
      }
    }
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val path = toPath(row)
    if (client.checkExists().forPath(path) == null) { None } else {
      Option(client.getData.forPath(path))
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    import scala.collection.JavaConversions._
    val path = prefix.map(toPath(_, withSlash = false))
    if (client.checkExists().forPath(Root) == null) { CloseableIterator.empty } else {
      val all = CloseableIterator(client.getChildren.forPath(Root).iterator)
      val filtered = path match {
        case None => all
        case Some(p) => all.filter(_.startsWith(p))
      }
      filtered.map { f =>
        val bytes = f.getBytes(StandardCharsets.UTF_8)
        (bytes, client.getData.forPath(toPath(bytes)))
      }
    }
  }

  override def close(): Unit = client.close()

  private def toPath(row: Array[Byte], withSlash: Boolean = true): String = {
    val string = new String(row, StandardCharsets.UTF_8) // TODO escape?
    if (withSlash) { "/" + string } else { string }
  }
}

object ZookeeperMetadata {
  private val Root = "/"
}
