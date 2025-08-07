/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.geotools.util.factory.Hints

/** Maintains Key -> String and String -> Key mappings */
object HintKeySerialization {

  private val MasterHints = Map(
    Hints.PROVIDED_FID     -> ("PROVIDED_FID", 0),
    Hints.USE_PROVIDED_FID -> ("USE_PROVIDED_FID", 1)
  )

  // Add more keys as needed.
  val keyToId: Map[Hints.Key, String] = MasterHints.map { case (k, (v, _)) => (k, v) }

  val idToKey: Map[String, Hints.Key] = keyToId.map(_.swap)

  val keyToEnum: Map[Hints.Key, Int] = MasterHints.map { case (k, (_, v)) => (k, v) }

  val enumToKey: Map[Int, Hints.Key] = keyToEnum.map(_.swap)

  def canSerialize(key: Hints.Key): Boolean = MasterHints.contains(key)
}
