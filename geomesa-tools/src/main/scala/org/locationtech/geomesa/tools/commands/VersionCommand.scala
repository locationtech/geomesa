/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.tools.commands

import java.util.Properties

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.tools.commands.VersionCommand._

class VersionCommand(parent: JCommander) extends Command(parent) {

  override val command = "version"
  override val params = new VersionParameters

  override def execute() = {
    val properties = new Properties()
    val stream = getClass
                  .getClassLoader
                  .getResourceAsStream(propertiesPath)

    properties.load(stream)
    println(s"GeoMesa Version ${properties.getProperty("GEOMESA_BUILD_VERSION")} " +
            s"built on ${properties.getProperty("GEOMESA_BUILD_DATE")}")
    stream.close()
  }
}

object VersionCommand {
  val propertiesPath ="org/locationtech/geomesa/tools/geomesaVersion.properties"
  @Parameters(commandDescription = "GeoMesa Version")
  class VersionParameters {}
}
