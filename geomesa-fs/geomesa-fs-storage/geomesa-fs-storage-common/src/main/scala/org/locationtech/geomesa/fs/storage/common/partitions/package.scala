/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.nio.charset.StandardCharsets
import java.util.Locale

package object partitions {

  // used to create upper bounds based on a prefix
  // note: we use 1 instead of 0 b/c 0 is not a valid char in postgres so breaks jdbc metadata filtering
  val ZeroChar = new String(Array[Byte](1), StandardCharsets.UTF_8)

  case class SchemeOpts(name: String, opts: Map[String, String], multiOpts: Map[String, Seq[String]]) {
    def getSingle(k: String): Option[String] = {
      if (multiOpts.contains(k)) {
        throw new IllegalArgumentException(s"Expected a single value for '$k' but got: ${multiOpts(k).mkString(", ")}")
      }
      opts.get(k)
    }

    def getMulti(k: String): Seq[String] = multiOpts.get(k).orElse(opts.get(k).map(v => Seq(v))).getOrElse(Seq.empty)
  }

  object SchemeOpts {
    def apply(scheme: String): SchemeOpts = {
      val parts = scheme.split(":")
      val name = parts.head.toLowerCase(Locale.US)
      val opts = parts.drop(1).map(_.split("=", 2) match { case Array(k, v) => k.toLowerCase(Locale.US) -> v }).groupBy(_._1)
      val singleOpts = opts.collect { case (k, Array(v)) => k -> v._2 }
      val multiOpts = opts.collect { case (k, v) if v.lengthCompare(1) > 0 => k -> v.map(_._2).toSeq }
      SchemeOpts(name, singleOpts, multiOpts)
    }
  }
}
