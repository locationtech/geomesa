/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander._
import com.beust.jcommander.converters.IParameterSplitter
import org.locationtech.geomesa.kafka.{KafkaDataStore, KafkaDataStoreSchemaManager}
import org.locationtech.geomesa.tools.kafka.commands.KeywordCommand.KeywordParameters
import org.locationtech.geomesa.tools.common.OptionalFeatureTypeNameParam
import org.locationtech.geomesa.tools.kafka.ProducerKDSConnectionParams
import scala.collection.JavaConversions._
import scala.io.StdIn
import scala.util.{Try, Failure}

class KeywordCommand(parent: JCommander) extends CommandWithKDS(parent) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  override val command: String = "keyword"
  override val params = new KeywordParameters()

  override def execute(): Unit = {

    if (ds == null) {
      throw new ParameterException("Could not load a data store with the provided parameters")
    }

    val sft = ds.asInstanceOf[KafkaDataStore].getKafkaSchema(params.featureName)
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

    // Attempt to replace the old schema with the updated one
    if (keywordsModified) {
      Try {
        ds.removeSchema(sft.getTypeName)
      } match {
        case Failure(ex) => println(ex.toString)
      }
      ds.asInstanceOf[KafkaDataStoreSchemaManager].createSchema(sft)
    }

    ds.dispose()

    if (params.list) {
      val reloadedSft = ds.asInstanceOf[KafkaDataStore].getKafkaSchema(params.featureName)
      println("Keywords: " + reloadedSft.getKeywords.toString)
    }
  }
}

// Overrides JCommander's split on comma to allow single keywords containing commas
class KeywordParameterSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = List(s)
}

object KeywordCommand {

  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class KeywordParameters extends  ProducerKDSConnectionParams
    with OptionalFeatureTypeNameParam {

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



