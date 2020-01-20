/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

import scala.util.control.NonFatal

case class IndexId(name: String, version: Int, attributes: Seq[String], mode: IndexMode = IndexMode.ReadWrite) {

  lazy val encoded: String = s"$name:$version:${mode.flag}:${attributes.mkString(":")}"

  override def equals(other: Any): Boolean = other match {
    case that: IndexId => encoded == that.encoded
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(encoded)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object IndexId {

  /**
    * Parse a formatted id string
    *
    * @param s input string
    * @return
    */
  def apply(s: String): IndexId = {
    try {
      val Array(name, version, flag, attrs @ _*) = s.split(":")
      IndexId(name, version.toInt, attrs, IndexMode(flag.toInt))
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid index string: $s", e)
    }
  }

  /**
    * Parses an `identifier` from a feature index. The input should not have a read/write flag, but
    * just consist of `name:version:attributes`
    *
    * @param identifier identifier
    * @return
    */
  def id(identifier: String): IndexId = {
    val Array(name, version, attrs @ _*) = identifier.split(":")
    IndexId(name, version.toInt, attrs)
  }
}
