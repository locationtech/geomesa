/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.wcs

import java.awt.RenderingHints
import java.util.Collections

import org.geotools.coverage.grid.io.{AbstractGridFormat, GridFormatFactorySpi}

class AccumuloCoverageFormatFactory extends GridFormatFactorySpi {
  def isAvailable = true

  def createFormat(): AbstractGridFormat = new AccumuloCoverageFormat

  def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()

}
