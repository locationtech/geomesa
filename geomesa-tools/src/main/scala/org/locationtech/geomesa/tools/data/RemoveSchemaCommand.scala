/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import java.io.IOException
import java.util.regex.Pattern

import com.beust.jcommander.ParameterException
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.Prompt

trait RemoveSchemaCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name = "remove-schema"
  override def params: RemoveSchemaParams

  override def execute(): Unit = {
    (Option(params.pattern), Option(params.featureName)) match {
      case (None, None) => throw new ParameterException("Please provide either featureName or pattern")
      case (Some(_), Some(_)) => throw new ParameterException("Cannot specify both featureName and pattern")
      case (Some(pattern), None)  => withDataStore(remove(_, pattern))
      case (None, Some(typeName)) => withDataStore(remove(_, Seq(typeName)))
    }
  }

  protected def remove(ds: DS, pattern: Pattern): Unit = {
    val typeNames = ds.getTypeNames.filter(pattern.matcher(_).matches)
    if (typeNames.isEmpty) {
      Command.user.warn("No schemas matched the provided pattern")
    } else {
      remove(ds, typeNames)
    }
  }

  protected def remove(ds: DS, typeNames: Seq[String]): Unit = {
    if (params.force || promptConfirm(typeNames)) {
      typeNames.foreach { typeName =>
        if (try { ds.getSchema(typeName) == null } catch { case _: IOException => true }) {
          Command.user.warn(s"Schema '$typeName' doesn't exist")
        } else {
          Command.user.info(s"Removing '$typeName'")
          ds.removeSchema(typeName)
          if (try { ds.getSchema(typeName) != null } catch { case _: IOException => false }) {
            Command.user.error(s"Error removing feature type '$typeName'")
          }
        }
      }
    } else {
      Command.user.info(s"Cancelled schema removal")
    }
  }

  protected def promptConfirm(featureNames: Seq[String]): Boolean =
    Prompt.confirm(s"Remove schema(s) ${featureNames.mkString(", ")}? (yes/no): ")

}

// @Parameters(commandDescription = "Remove a schema and associated features from a GeoMesa catalog")
trait RemoveSchemaParams extends OptionalTypeNameParam with OptionalForceParam with OptionalPatternParam
