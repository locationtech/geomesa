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
package org.locationtech.geomesa.tools

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.{params => dsParams}
import org.locationtech.geomesa.tools.commands.GeoMesaParams

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DataStoreHelper(params: GeoMesaParams) extends AccumuloProperties {
  lazy val instance = Option(params.instance).getOrElse(instanceName)
  lazy val zookeepersString = Option(params.zookeepers).getOrElse(zookeepersProp)

  lazy val paramMap = Map[String, String](
    dsParams.instanceIdParam.getName -> instance,
    dsParams.zookeepersParam.getName -> zookeepersString,
    dsParams.userParam.getName       -> params.user,
    dsParams.passwordParam.getName   -> getPassword(params.password),
    dsParams.tableNameParam.getName  -> params.catalog,
    dsParams.visibilityParam.getName -> Option(params.visibilities).orNull,
    dsParams.authsParam.getName      -> Option(params.auths).orNull,
    dsParams.mockParam.getName       -> params.useMock.toString)

  lazy val ds: AccumuloDataStore =
    Try({ DataStoreFinder.getDataStore(paramMap).asInstanceOf[AccumuloDataStore] }) match {
      case Success(value) => value
      case Failure(ex)    =>
        val paramMsg = paramMap.map { case (k,v) => s"$k=$v" }.mkString(",")
        throw new Exception(s"Cannot connect to Accumulo. Please check your configuration: $paramMsg", ex)
    }
}
