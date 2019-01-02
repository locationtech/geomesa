/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CachedLazyMetadataTest extends Specification {

  "CachedLazyMetadata" should {
    "handle invalid rows in getTypeNames" in {
      val metadata = new TestCachedLazyMetadata
      metadata.getFeatureTypes mustEqual Array.empty
      metadata.tableExists.set(true)
      metadata.data.put("foo".getBytes(StandardCharsets.UTF_8), "foo".getBytes(StandardCharsets.UTF_8))
      metadata.getFeatureTypes mustEqual Array.empty
      metadata.insert("bar", GeoMesaMetadata.ATTRIBUTES_KEY, "bar")
      metadata.insert("bar", "bar", "bar")
      metadata.insert("baz", GeoMesaMetadata.ATTRIBUTES_KEY, "baz")
      metadata.insert("baz", "baz", "baz")
      metadata.getFeatureTypes mustEqual Array("bar", "baz")
    }
  }

  class TestCachedLazyMetadata extends CachedLazyBinaryMetadata[String] {

    lazy val tableExists = new AtomicBoolean(false)
    lazy val data =
      Collections.synchronizedMap(new java.util.TreeMap[Array[Byte], Array[Byte]](ByteArrays.ByteOrdering))

    override protected def serializer: MetadataSerializer[String] = MetadataStringSerializer

    override protected def checkIfTableExists: Boolean = tableExists.get

    override protected def createTable(): Unit = tableExists.set(true)

    override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit =
      rows.foreach { case (k, v) => data.put(k, v) }

    override protected def delete(rows: Seq[Array[Byte]]): Unit = rows.foreach(data.remove)

    override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = Option(data.get(row))

    override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
      import scala.collection.JavaConverters._
      prefix match {
        case None => CloseableIterator(data.asScala.toIterator)
        case Some(p) =>
          val filtered = data.asScala.toIterator.flatMap { case (k, v) =>
            if (p.length <= k.length && java.util.Arrays.equals(p, k.take(p.length))) {
              Iterator.single(k -> v)
            } else {
              Iterator.empty
            }
          }
          CloseableIterator(filtered)
      }
    }

    override def close(): Unit = {}
  }
}
