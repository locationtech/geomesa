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

import collection.JavaConversions._
import org.apache.accumulo.core.util.shell.{Command, Shell}
import org.apache.commons.cli.CommandLine
import geomesa.core.data.AccumuloDataStore

class ListFeaturesCommand extends Command {
  override def numArgs() = 0

  override def description() = "Lists GeoMesa features"

  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    val auths = shellState.getConnector().securityOperations().getUserAuthorizations(shellState.getConnector.whoami())
    val tables = shellState.getConnector.tableOperations().list()
    val features = tables.flatMap { table =>
      val ds = new AccumuloDataStore(shellState.getConnector, table, auths, "")
      ds.getTypeNames.map { t => "%s\t%s".format(table, t) }
    }
    shellState.printLines(features.iterator, true)
    0
  }
}
