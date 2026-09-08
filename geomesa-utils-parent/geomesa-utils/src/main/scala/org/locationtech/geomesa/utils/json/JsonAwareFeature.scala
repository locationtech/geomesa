/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.json

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.utils.json.JsonPathParser.JsonPath

trait JsonAwareFeature extends SimpleFeature {

  /**
   * Reads a json path out of a string-type json attribute
   *
   * @param attribute index of the attribute to read
   * @param path the path to read out of the attribute
   * @return
   */
  def readJsonPath(attribute: Int, path: JsonPath): Any
}
