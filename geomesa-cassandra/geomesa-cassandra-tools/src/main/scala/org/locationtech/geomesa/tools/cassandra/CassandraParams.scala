package org.locationtech.geomesa.tools.cassandra

import com.beust.jcommander.Parameter

/**
  * Created by etalbot on 10/18/16.
  */
trait CassandraConnectionParams {
  @Parameter(names = Array("--contact-point"), description = "contact-point")
  var contactPoint: String = null

  @Parameter(names = Array("--key-space"), description = "key-space")
  var keySpace: String = null
}
