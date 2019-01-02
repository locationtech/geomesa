/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.AgeOffCommand.AgeOffParams
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.tools.utils.Prompt

import scala.concurrent.duration.Duration

trait AgeOffCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name = "configure-age-off"
  override def params: AgeOffParams

  override def execute(): Unit = {
    if (Seq(params.set, params.remove, params.list).count(_ == true) != 1) {
      throw new ParameterException("Must specify exactly one of 'list', 'set' or 'remove'")
    } else if (params.set && params.expiry == null) {
      throw new ParameterException("Must specify 'expiry' when setting age-off")
    }

    if (params.list) {
      withDataStore(list(_, params.featureName))
    } else if (params.set) {
      if (params.dtgField == null) {
        val confirm = Prompt.confirm(s"Configuring ingest-time-based age-off for schema '${params.featureName}' " +
            s"with expiry ${params.expiry}. Continue? (y/n): ")
        if (confirm) {
          withDataStore(set(_, params.featureName, params.expiry))
        }
      } else {
        val confirm = Prompt.confirm(s"Configuring attribute-based age-off for schema '${params.featureName}' " +
            s"on field '${params.dtgField}' with expiry ${params.expiry}. Continue? (y/n): ")
        if (confirm) {
          withDataStore(set(_, params.featureName, params.dtgField, params.expiry))
        }
      }
    } else {
      val confirm = Prompt.confirm(s"Removing age-off for schema '${params.featureName}'. Continue? (y/n): ")
      if (confirm) {
        withDataStore(remove(_, params.featureName))
      }
    }
  }

  protected def list(ds: DS, featureName: String): Unit

  protected def set(ds: DS, featureName: String, expiry: Duration): Unit

  protected def set(ds: DS, featureName: String, dtg: String, expiry: Duration): Unit

  protected def remove(ds: DS, featureName: String): Unit
}

object AgeOffCommand {
  trait AgeOffParams extends RequiredTypeNameParam with OptionalDtgParam {
    @Parameter(names = Array("-e", "--expiry"), description = "Duration before entries are aged-off, e.g. '1 day', '2 weeks and 1 hour', etc", converter = classOf[DurationConverter])
    var expiry: Duration = _

    @Parameter(names = Array("-l", "--list"), description = "List existing age-off for a simple feature type")
    var list: Boolean = _

    @Parameter(names = Array("-s", "--set"), description = "Set age-off for a simple feature type")
    var set: Boolean = _

    @Parameter(names = Array("-r", "--remove"), description = "Remove existing age-off for a simple feature type")
    var remove: Boolean = _
  }
}