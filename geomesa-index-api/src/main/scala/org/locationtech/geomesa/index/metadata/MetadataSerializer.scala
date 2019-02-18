/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.nio.charset.StandardCharsets

trait MetadataSerializer[T] {
  def serialize(typeName: String, value: T): Array[Byte]
  def deserialize(typeName: String, value: Array[Byte]): T
}

object MetadataStringSerializer extends MetadataSerializer[String] {
  def serialize(typeName: String, value: String): Array[Byte] = {
    if (value == null) Array.empty else value.getBytes(StandardCharsets.UTF_8)
  }
  def deserialize(typeName: String, value: Array[Byte]): String = {
    if (value.isEmpty) null else new String(value, StandardCharsets.UTF_8)
  }
}
