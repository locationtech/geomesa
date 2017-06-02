/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.data

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, SimpleProducerKDSConnectionParams}
import org.locationtech.geomesa.kafka08.KafkaDataStore
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, OptionalForceParam, OptionalPatternParam, OptionalTypeNameParam}

import scala.util.{Failure, Success, Try}

class KafkaRemoveSchemaCommand extends KafkaDataStoreCommand {

  override val name = "remove-schema"
  override val params = new KafkaRemoveSchemaParams

  override def execute(): Unit = {
    if (Option(params.pattern).isEmpty && Option(params.featureName).isEmpty) {
      throw new IllegalArgumentException("Please provide either the featureName or pattern parameter to remove schemas.")
    } else if (Option(params.pattern).isDefined && Option(params.featureName).isDefined) {
      throw new IllegalArgumentException("Cannot specify both the featureName and pattern parameter to remove schemas.")
    }

    withDataStore { (ds) =>
    val typeNamesToRemove = getTypeNamesFromParams(ds)
      validate(ds, typeNamesToRemove) match {
        case Success(_) =>
          if (params.force || promptConfirm(typeNamesToRemove)) {
            removeAll(ds, typeNamesToRemove)
          } else {
            Command.user.info(s"Cancelled schema removal.")
          }
        case Failure(ex) =>
          Command.user.error(s"Feature validation failed on error: ${ex.getMessage}.")
      }
    }
  }

  protected def removeAll(ds: KafkaDataStore, typeNames: List[String]) = {
    typeNames.foreach { tname =>
      Try {
        ds.removeSchema(tname)
        if (ds.getNames.contains(tname)) {
          throw new Exception(s"Error removing feature type '${params.zkPath}:$tname'.")
        }
      } match {
        case Success(_) =>
          Command.user.info(s"Removed $tname")
        case Failure(ex) =>
          Command.user.error(s"Failure removing type $tname")
      }
    }
  }

  protected def getTypeNamesFromParams(ds: KafkaDataStore) =
    Option(params.featureName).toList ++ Option(params.pattern).map { p =>
      ds.getTypeNames.filter(p.matcher(_).matches).toList
    }.getOrElse(List.empty[String])

  protected def validate(ds: KafkaDataStore, typeNames: List[String]) = Try {
    if (typeNames.isEmpty) {
      throw new IllegalArgumentException("No feature type names found from pattern or provided feature type name.")
    }

    val validFeatures = ds.getTypeNames
    typeNames.foreach { f =>
      if (!validFeatures.contains(f)) {
        throw new IllegalArgumentException(s"Feature type name $f does not exist at zkPath ${params.zkPath}")
      }
    }
  }

  protected def promptConfirm(featureNames: List[String]) =
    Prompt.confirm(s"Remove schema(s) ${featureNames.mkString(",")} at zkPath ${params.zkPath}? (yes/no): ")
}

@Parameters(commandDescription = "Remove a schema and associated features from GeoMesa")
class KafkaRemoveSchemaParams extends SimpleProducerKDSConnectionParams
  with OptionalTypeNameParam with OptionalForceParam with OptionalPatternParam
