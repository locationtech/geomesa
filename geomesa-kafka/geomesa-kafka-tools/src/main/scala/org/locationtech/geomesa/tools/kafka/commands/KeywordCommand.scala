/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander._
import org.locationtech.geomesa.kafka08.KafkaDataStoreSchemaManager
import org.locationtech.geomesa.tools.kafka.commands.KeywordCommand.KeywordParameters
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, KeywordParamSplitter}
import org.locationtech.geomesa.tools.kafka.ProducerKDSConnectionParams

import scala.collection.JavaConversions._
import scala.io.StdIn

class KeywordCommand(parent: JCommander) extends CommandWithKDS(parent) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  override val command: String = "keywords"
  override val params = new KeywordParameters()

  override def execute(): Unit = {

    val sft = ds.getSchema(params.featureName)

    var keywordsModified = false

    if (params.keywordsToAdd != null) {
      sft.addKeywords(params.keywordsToAdd.mkString(KEYWORDS_DELIMITER))
      keywordsModified = true
    }

    if (params.keywordsToRemove != null) {
      sft.removeKeywords(params.keywordsToRemove.mkString(KEYWORDS_DELIMITER))
      keywordsModified = true
    }

    if (params.removeAll) {
      val confirm = StdIn.readLine("Remove all keywords? (y/n): ").toLowerCase
      if (confirm.equals("y") || confirm.equals("yes")) {
        sft.removeAllKeywords()
        keywordsModified = true
      } else {
        println("Aborting operation")
        ds.dispose()
        return
      }
    }

    // Update the existing schema
    ds.asInstanceOf[KafkaDataStoreSchemaManager].updateKafkaSchema(sft.getTypeName, sft)

    if (params.list) {
      val reloadedSft = ds.getSchema(params.featureName)
      println("Keywords: " + reloadedSft.getKeywords.toString)
    }

    ds.dispose()
  }
}

object KeywordCommand {

  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class KeywordParameters extends  ProducerKDSConnectionParams
    with FeatureTypeNameParam {

    @Parameter(names = Array("-a", "--add"), description = "A keyword to add. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
    var keywordsToAdd: java.util.List[String] = null

    @Parameter(names = Array("-r", "--remove"), description = "A keyword to remove. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
    var keywordsToRemove: java.util.List[String] = null

    @Parameter(names = Array("-l", "--list"), description = "List all keywords on the schema")
    var list: Boolean = false

    @Parameter(names = Array("--removeAll"), description = "Remove all keywords on the schema")
    var removeAll: Boolean = false

  }

}



