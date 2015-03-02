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
package org.locationtech.geomesa.tools.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.DataStoreHelper
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.commands.IngestCommand._
import org.locationtech.geomesa.tools.ingest.DelimitedIngest
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest

import scala.collection.JavaConversions._
import scala.collection.mutable

class IngestCommand(parent: JCommander) extends Command(parent) with Logging {
  override val command = "ingest"
  override val params = new IngestParameters()

  override def execute(): Unit = {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.files(0)))
    fmt match {
      case CSV | TSV => new DelimitedIngest(params).run()
      case SHP       =>
        val ds = new DataStoreHelper(params).getOrCreateDs
        GeneralShapefileIngest.shpToDataStore(params.files(0), ds, params.featureName)
      case _         =>
        logger.error("Error: File format not supported for file " + params.files(0) + ". Supported formats" +
          "are csv,tsv,shp")
    }
  }

}

object IngestCommand {
  @Parameters(commandDescription = "Ingest a file of various formats into GeoMesa")
  class IngestParameters extends CreateFeatureParams {
    @Parameter(names = Array("-is", "--index-schema"), description = "GeoMesa index schema format string")
    var indexSchema: String = null

    @Parameter(names = Array("-cols", "--columns"), description = "the set of column indexes to be ingested, " +
      "must match the SimpleFeatureType spec (zero-indexed)")
    var columns: String = null

    @Parameter(names = Array("-dtf", "--dt-format"), description = "format string for the date time field")
    var dtFormat: String = null

    @Parameter(names = Array("-id", "--id-fields"), description = "the set of attributes to combine together to " +
      "create a unique id for the feature (comma separated)")
    var idFields: String = null

    @Parameter(names = Array("-h", "--hash"), description = "flag to toggle using md5hash as the feature id")
    var hash: Boolean = false

    @Parameter(names = Array("-lat", "--lat-attribute"), description = "name of the latitude field in the " +
      "SimpleFeature if longitude is kept in the SFT spec; otherwise defines the csv field index used to create " +
      "the default geometry")
    var lat: String = null

    @Parameter(names = Array("-lon", "--lon-attribute"), description = "name of the longitude field in the " +
      "SimpleFeature if longitude is kept in the SFT spec; otherwise defines the csv field index used to create " +
      "the default geometry")
    var lon: String = null

    @Parameter(names = Array("-fmt", "--format"), description = "format of incoming data (csv | tsv | shp) " +
      "to override file extension recognition")
    var format: String = null

    @Parameter(names = Array("--skip-header"), description = "flag to skip the first line (header) of a csv/tsv data file")
    var skipHeader: Boolean = false

    @Parameter(names = Array("-ld", "--list-delimiter"), description = "character(s) to delimit list features")
    var listDelimiter: String = ","

    @Parameter(names = Array("-md", "--map-delimiters"), arity = 2, description = "characters to delimit map features, first to divide keys from values, then to divide between key/value pairs")
    var mapDelimiters: java.util.List[String] = new util.ArrayList[String](Seq(",", ";"))

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }
}
