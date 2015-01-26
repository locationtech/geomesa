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
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.{AccumuloProperties, DataStoreHelper}

/**
 * Abstract superclass for all top-level GeoMesa Jcommander commands
 */
abstract class Command(parent: JCommander) {
  def execute()
  val params: Any
  def register = parent.addCommand(command, params)
  val command: String
}

/**
 * Abstract class for commands that have a pre-existing catlaog
 */
abstract class CommandWithCatalog(parent: JCommander) extends Command(parent) with AccumuloProperties {
  override val params: GeoMesaParams
  lazy val ds = new DataStoreHelper(params).getExistingStore
  lazy val catalog = params.catalog
}
