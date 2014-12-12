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

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.FeatureCreator
import org.locationtech.geomesa.tools.commands.CreateCommand.{Command, CreateParameters}

class CreateCommand(parent: JCommander) extends Command with Logging {

  val params = new CreateParameters()
  parent.addCommand(Command, params)

  override def execute() = FeatureCreator.createFeature(params)
}

object CreateCommand {
  val Command = "create"

  @Parameters(commandDescription = "Create a feature definition in a GeoMesa catalog")
  class CreateParameters extends CreateFeatureParams {}
}