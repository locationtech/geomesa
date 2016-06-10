/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander._
import com.beust.jcommander.converters.IParameterSplitter
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.tools.accumulo.commands.KeywordCommand.KeywordParameters
import org.locationtech.geomesa.tools.accumulo.{DataStoreHelper, GeoMesaConnectionParams}
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.tools.common.commands.Command

import scala.collection.JavaConversions._
import scala.io.StdIn

class KeywordCommand(parent: JCommander) extends CommandWithCatalog(parent) {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  override val command: String = "keywords"
  override val params = new KeywordParameters()

  override def execute(): Unit = {

    if (ds == null) {
      throw new ParameterException("Could not load a data store with the provided parameters")
    }

    val sft = ds.getSchema(params.featureName)

    if (params.keywordsToAdd != null) {
      sft.addKeywords(params.keywordsToAdd.mkString(KEYWORDS_DELIMITER))
    }

    if (params.keywordsToRemove != null) {
      sft.removeKeywords(params.keywordsToRemove.mkString(KEYWORDS_DELIMITER))
    }

    if (params.removeAll) {
      val confirm = StdIn.readLine("Remove all keywords? (y/n): ").toLowerCase
      if (confirm.equals("y") || confirm.equals("yes")) {
        sft.removeAllKeywords()
      } else {
        println("Aborting operation")
        ds.dispose()
        return
      }
    }

    ds.updateSchema(params.featureName, sft)

    if (params.list) {
      println("Keywords: " + ds.getSchema(sft.getTypeName).getKeywords.toString)
    }

    ds.dispose()
  }
}

// Overrides JCommander's split on comma to allow single keywords containing commas
class KeywordParameterSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = List(s)
}

object KeywordCommand {

  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class KeywordParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam {

    @Parameter(names = Array("-a", "--add"), description = "A keyword to add. Can be specified multiple times", splitter = classOf[KeywordParameterSplitter])
    var keywordsToAdd: java.util.List[String] = null

    @Parameter(names = Array("-r", "--remove"), description = "A keyword to remove. Can be specified multiple times", splitter = classOf[KeywordParameterSplitter])
    var keywordsToRemove: java.util.List[String] = null

    @Parameter(names = Array("-l", "--list"), description = "List all keywords on the schema")
    var list: Boolean = false

    @Parameter(names = Array("--removeAll"), description = "Remove all keywords on the schema")
    var removeAll: Boolean = false

  }

}



