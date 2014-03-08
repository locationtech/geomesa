/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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
package geomesa.core.util.shell

import org.apache.accumulo.core.util.shell.{Shell, Command}
import org.apache.commons.cli.{Option => Opt, Options, CommandLine}
import org.geotools.data.{DataUtilities, DataStoreFinder}
import geomesa.core.iterators.SpatioTemporalIntersectingIterator
import geomesa.core.index.Constants

class InitializeFeatureCommand extends Command {
  val schemaOpt = new Opt("is", "indexschema", true, "Custom index schema")
  val dtgOpt    = new Opt("dtg", "datetimefield", true, "Field to use for datetime index")

  override def numArgs() = 3

  override def description() = "Create a new GeoMesa feature table with the specified feature type"

  import collection.JavaConversions._
  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    SpatioTemporalIntersectingIterator.initClassLoader(null)

    val conn = shellState.getConnector
    val auths = conn.securityOperations().getUserAuthorizations(conn.whoami()).toString
    val args = cl.getArgs
    val tableName = args(0)
    val featureName = args(1)
    val sftSpec = args(2)

    val params = Map("connector" -> conn, "tableName" -> tableName, "auths" -> auths)
    val finalParams = cl.getOpt(schemaOpt).map { isf => params + ("indexSchemaFormat" -> isf) }.getOrElse(params)

    val ds = DataStoreFinder.getDataStore(finalParams)

    val sft = DataUtilities.createType(featureName, sftSpec)
    cl.getOpt(dtgOpt).foreach { arg =>
      sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, arg)
      sft.getUserData.put(Constants.SF_PROPERTY_END_TIME,   arg)
    }
    ds.createSchema(sft)

    0
  }

  override def getOptions: Options = {
    val options = super.getOptions
    options.addOption(schemaOpt)
    options.addOption(dtgOpt)
    options
  }
}
