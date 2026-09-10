/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.json.JsonPathParser
import org.locationtech.geomesa.utils.json.JsonPathParser.{JsonPath, PathAttribute}

import scala.util.control.NonFatal

package object iceberg {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  /**
   * Parse a json path
   *
   * @param pathString path
   * @param sft feature type
   * @return descriptor indicated by the head of the path, and then the rest of the path pointing into the attribute
   */
  def parseJsonPath(pathString: String, sft: SimpleFeatureType): (AttributeDescriptor, JsonPath) = {
    val path = try { JsonPathParser.parse(pathString) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Could not evaluate as a JSON path: $pathString", e)
    }
    if (path.isEmpty) {
      throw new IllegalArgumentException(s"Invalid JSON path - empty: $pathString")
    }
    val descriptor = path.head match {
      case PathAttribute(name, _) =>
        val descriptor = sft.getDescriptor(name)
        if (descriptor == null) {
          throw new IllegalArgumentException(s"Invalid JSON path - does not point at an attribute: $pathString")
        } else if (!classOf[String].isAssignableFrom(descriptor.getType.getBinding)) {
          throw new IllegalArgumentException(
            s"Invalid JSON path - points at an invalid attribute of type ${descriptor.getType.getBinding.getSimpleName}: $pathString")
        } else if (!descriptor.isJson()) {
          throw new IllegalArgumentException(s"Invalid JSON path - points at a non-JSON attribute: $pathString")
        }
        descriptor

      case _ =>
        throw new IllegalArgumentException(s"Invalid JSON path - first element must point at an attribute: $pathString")
    }

    (descriptor, path.tail)
  }
}
