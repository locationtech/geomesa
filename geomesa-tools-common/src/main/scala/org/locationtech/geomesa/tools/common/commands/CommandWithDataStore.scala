package org.locationtech.geomesa.tools.common.commands

import org.geotools.data.DataStore

trait CommandWithDataStore {
  val ds: DataStore
}