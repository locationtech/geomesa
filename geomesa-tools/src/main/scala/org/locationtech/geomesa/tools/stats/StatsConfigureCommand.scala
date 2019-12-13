/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.stats.StatsConfigureCommand._

trait StatsConfigureCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "configure-stats"
  override def params: StatsConfigureParams

  override def execute(): Unit = {
    if (Seq(params.add, params.remove, params.list).count(_ == true) != 1) {
      throw new ParameterException("Must specify exactly one of 'list', 'add' or 'remove'")
    }

    if (params.list) {
      withDataStore(list)
    } else if (params.add) {
      withDataStore(add)
    } else {
      withDataStore(remove)
    }
  }

  protected def list(ds: DS): Unit

  protected def add(ds: DS): Unit

  protected def remove(ds: DS): Unit
}

object StatsConfigureCommand {

  trait StatsConfigureParams {
    @Parameter(names = Array("-l", "--list"), description = "List current stats table configuration for a catalog")
    var list: Boolean = _

    @Parameter(names = Array("-a", "--add"), description = "Configure the stats table for a catalog")
    var add: Boolean = _

    @Parameter(names = Array("-r", "--remove"), description = "Remove current stats table configuration for a catalog")
    var remove: Boolean = _
  }
}
