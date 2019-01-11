/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.nio.charset.StandardCharsets
import java.util

import org.apache.accumulo.core.client.{IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.utils.conf.GeoMesaProperties

class ProjectVersionIterator extends SortedKeyValueIterator[Key, Value] {

  private var result = false

  override def hasTop: Boolean = !result

  override def next(): Unit = result = true

  override def getTopValue: Value =
    new Value(GeoMesaProperties.ProjectVersion.getBytes(StandardCharsets.UTF_8))

  override def getTopKey: Key = new Key()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.result = false
  }

  override def seek(range: Range,
                    columnFamilies: util.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {}

  override def deepCopy(env: IteratorEnvironment) = throw new NotImplementedError()
}

object ProjectVersionIterator {

  def configure(): IteratorSetting = new IteratorSetting(30, classOf[ProjectVersionIterator])

  def scanProjectVersion(scanner: Scanner): Set[String] = {
    import scala.collection.JavaConversions._
    scanner.addScanIterator(configure())
    scanner.iterator.map(e => new String(e.getValue.get, StandardCharsets.UTF_8)).toSet
  }
}
