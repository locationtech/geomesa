/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.RemoveSchemaCommand.RemoveSchemaParams
import org.locationtech.geomesa.tools.common.{OptionalFeatureTypeNameParam, OptionalForceParam, OptionalPatternParam, Prompt}

import scala.util.{Failure, Success, Try}

class RemoveSchemaCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "remove-schema"
  override val params = new RemoveSchemaParams

  override def execute() = {
    if (Option(params.pattern).isEmpty && Option(params.featureName).isEmpty) {
      throw new IllegalArgumentException("Please provide either featureName or pattern to removing.")
    } else if (Option(params.pattern).isDefined && Option(params.featureName).isDefined) {
      throw new IllegalArgumentException("Cannot specify both the featureName and pattern parameter to remove schemas.")
    }

    val typeNamesToRemove = getTypeNamesFromParams()
    validate(typeNamesToRemove) match {
      case Success(_) =>
        if (params.force || promptConfirm(typeNamesToRemove)) {
          removeAll(typeNamesToRemove)
        } else {
          logger.info(s"Cancelled schema removal.")
        }
      case Failure(ex) =>
        println(s"Feature validation failed on error: ${ex.getMessage}.")
    }
    ds.dispose()
  }

  protected def removeAll(typeNames: List[String]) = {
    typeNames.foreach { tname =>
      Try {
        ds.removeSchema(tname)
        if (ds.getNames.contains(tname)) {
          throw new Exception(s"Error removing feature type '$catalog:$tname'.")
        }
      } match {
        case Success(_) =>
          println(s"Removed $tname")
        case Failure(ex) =>
          println(s"Failure removing type $tname")
      }
    }
  }

  protected def getTypeNamesFromParams() =
    Option(params.featureName).toList ++ Option(params.pattern).map { p =>
      ds.getTypeNames.filter(p.matcher(_).matches).toList
    }.getOrElse(List.empty[String])

  protected def validate(typeNames: List[String]) = Try {
    if (typeNames.isEmpty) {
      throw new IllegalArgumentException("No feature type names found from pattern or provided feature type name.")
    }

    val validFeatures = ds.getTypeNames
    typeNames.foreach { f =>
      if (!validFeatures.contains(f)) {
        throw new IllegalArgumentException(s"Feature type name $f does not exist in catalog $catalog.")
      }
    }
  }

  protected def promptConfirm(featureNames: List[String]) =
    Prompt.confirm(s"Remove schema(s) ${featureNames.mkString(",")} from catalog $catalog? (yes/no): ")

}

object RemoveSchemaCommand {
  @Parameters(commandDescription = "Remove a schema and associated features from a GeoMesa catalog")
  class RemoveSchemaParams extends GeoMesaConnectionParams
    with OptionalFeatureTypeNameParam
    with OptionalForceParam
    with OptionalPatternParam {}
}
