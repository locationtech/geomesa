/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import com.beust.jcommander.{JCommander, Parameters, ParametersDelegate}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.DeleteCatalogCommand._

class DeleteCatalogCommand (parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "deletecatalog"
  override val params = new DeleteCatalogParams

  override def execute() = {
    val msg = s"Delete catalog '$catalog'? (yes/no): "
    if (PromptConfirm.confirm(msg, List("yes", "y"))) {
      ds.delete()
      println(s"Deleted catalog $catalog")
    } else {
      logger.info(s"Cancelled deletion")
    }
  }
}

object DeleteCatalogCommand {

  @Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
  class DeleteCatalogParams extends GeoMesaParams {
    @ParametersDelegate
    val forceParams = new ForceParams
    def force = forceParams.force
  }

}
