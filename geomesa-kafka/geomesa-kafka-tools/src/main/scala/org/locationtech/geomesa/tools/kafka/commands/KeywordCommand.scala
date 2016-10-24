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
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, KeywordParamSplitter}
import org.locationtech.geomesa.tools.kafka.ProducerKDSConnectionParams
import org.locationtech.geomesa.tools.kafka.commands.KeywordCommand.KeywordParameters

class KeywordCommand(parent: JCommander) extends CommandWithKDS(parent) {

  override val command: String = "keywords"
  override val params = new KeywordParameters()

  override def execute(): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    import scala.collection.JavaConversions._

    val sft = ds.getSchema(params.featureName)

    var keywordsModified = false

    if (params.removeAll) {
      val confirm = System.console().readLine("Remove all keywords? (y/n): ").toLowerCase
      if (confirm.equals("y") || confirm.equals("yes")) {
        sft.removeAllKeywords()
        keywordsModified = true
      } else {
        println("Aborting operation")
        ds.dispose()
        return
      }
    } else if (params.keywordsToRemove != null) {
      sft.removeKeywords(params.keywordsToRemove.toSet)
      keywordsModified = true
    }

    if (params.keywordsToAdd != null) {
      sft.addKeywords(params.keywordsToAdd.toSet)
      keywordsModified = true
    }

    // Update the existing schema
    ds.asInstanceOf[KafkaDataStoreSchemaManager].updateKafkaSchema(sft.getTypeName, sft)

    if (params.list) {
      val reloadedSft = ds.getSchema(params.featureName)
      println("Keywords: " + reloadedSft.getKeywords.mkString(", "))
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



