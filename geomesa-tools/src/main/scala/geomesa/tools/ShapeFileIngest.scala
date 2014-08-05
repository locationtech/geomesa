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
package geomesa.tools

import java.net.URL
import geomesa.core.index.Constants
import org.geotools.data.shapefile.ShapefileDataStore

class ShapeFileIngest(config: Config, dsConfig: Map[String, _]) {
  lazy val table          = config.table
  lazy val path             = new URL(config.file)
  lazy val typeName         = config.typeName
  lazy val dtgTargetField   = Constants.SF_PROPERTY_START_TIME // sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val zookeepers       = dsConfig.get("zookeepers")
  lazy val user             = dsConfig.get("user")
  lazy val password         = dsConfig.get("password")
  lazy val auths            = dsConfig.get("auths")

  val shapeStore = new ShapefileDataStore(path)
  val name = shapeStore.getTypeNames()(0)

  //lazy val sft = SimpleFeatureTypes.createType(typeName, sftSpec)


}
