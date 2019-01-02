/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.geotools.factory.Hints

/** Maintains Key -> String and String -> Key mappings */
object HintKeySerialization {

  // Add more keys as needed.
  val keyToId: Map[Hints.Key, String] = Map(
    Hints.PROVIDED_FID -> "PROVIDED_FID",
    Hints.USE_PROVIDED_FID -> "USE_PROVIDED_FID"
  )

  val idToKey: Map[String, Hints.Key] = keyToId.map(_.swap)

  def canSerialize(key: Hints.Key): Boolean = keyToId.contains(key)
}
