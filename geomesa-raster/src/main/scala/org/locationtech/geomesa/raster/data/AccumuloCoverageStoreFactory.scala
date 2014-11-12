/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.raster.data

import java.io.Serializable
import java.util.{Map => JMap}
import org.apache.accumulo.core.client.Connector
import org.locationtech.geomesa.core.stats.StatWriter
import scala.util.Try

class AccumuloCoverageStoreFactory {

  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
  import org.locationtech.geomesa.raster.AccumuloStoreHelper

  def createCoverageStore(params: JMap[String, Serializable]) = {
    val visibility = AccumuloStoreHelper.getVisibility(params)
    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])
    val connector =
      if (params.containsKey(connParam.key)) connParam.lookUp(params).asInstanceOf[Connector]
      else AccumuloStoreHelper.buildAccumuloConnector(params, useMock)
    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(params, connector)
    val collectStats = !useMock && Try(statsParam.lookUp(params).asInstanceOf[java.lang.Boolean] == true).getOrElse(false)

    if (collectStats) {
      new AccumuloCoverageStore(connector,
                                tableName,
                                authorizationsProvider,
                                visibility,
                                shardsParam.lookupOpt(params),
                                writeMemoryParam.lookupOpt(params),
                                writeThreadsParam.lookupOpt(params),
                                queryThreadsParam.lookupOpt(params)) with StatWriter
    } else {
      new AccumuloCoverageStore(connector,
                                tableName,
                                authorizationsProvider,
                                visibility,
                                shardsParam.lookupOpt(params),
                                writeMemoryParam.lookupOpt(params),
                                writeThreadsParam.lookupOpt(params),
                                queryThreadsParam.lookupOpt(params))
    }
  }
}

