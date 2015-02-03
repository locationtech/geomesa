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
import org.locationtech.geomesa.tools.commands.DeleteFeatureCommand.DeleteFeatureParams

import scala.util.{Failure, Success, Try}

class DeleteFeatureCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "deletefeature"
  override val params = new DeleteFeatureParams

  override def execute() = {
    if (Option(params.pattern).isEmpty && Option(params.featureName).isEmpty) {
      throw new IllegalArgumentException("Error: Must provide either featureName or pattern to delete.")
    }

    val features = getFeatureList()
    validate(features) match {
      case Success(_) =>
        if (params.force || promptConfirm(features)) {
          removeAll(features)
        } else {
          logger.info(s"Cancelled deletion.")
        }
      case Failure(ex) =>
        println(s"Feature validation failed on error: ${ex.getMessage}.")
        logger.error(ex.getMessage)
    }
  }

  def removeAll(features: List[String]) = {
    features.foreach { f =>
      remove(f) match {
        case Success(_) =>
          println(s"Deleted feature $f")
        case Failure(ex) =>
          println(s"Failure deleting feature $f")
          logger.error(s"Failure deleting feature $f.", ex)
      }
    }
  }

  def remove(feature: String) =
    Try {
      ds.removeSchema(feature)
      if (ds.getNames.contains(feature)) {
        throw new Exception(s"There was an error deleting feature '$catalog:$feature'.")
      }
    }

  def getFeatureList() =
    Option(params.featureName).toList ++ Option(params.pattern).map { p =>
      ds.getTypeNames.filter(p.matcher(_).matches).toList
    }.getOrElse(List.empty[String])

  def validate(features: List[String]) = Try {
    if (features.isEmpty) {
      throw new IllegalArgumentException("No features found from pattern or feature name.")
    }

    val validFeatures = ds.getTypeNames
    features.foreach { f =>
      if (!validFeatures.contains(f)) {
        throw new IllegalArgumentException(s"Feature $f does not exist in catalog $catalog.")
      }
    }
  }

  def promptConfirm(features: List[String]) =
    PromptConfirm.confirm(s"Delete ${features.mkString(",")} from catalog $catalog? (yes/no): ")

}

object DeleteFeatureCommand {

  @Parameters(commandDescription = "Delete a feature's data and definition from a GeoMesa catalog")
  class DeleteFeatureParams extends OptionalFeatureParams {
    @ParametersDelegate
    val forceParams = new ForceParams
    def force = forceParams.force

    @ParametersDelegate
    val patternParams = new PatternParams
    def pattern = patternParams.pattern
  }

}
