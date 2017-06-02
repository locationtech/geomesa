/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander._
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, ProducerKDSConnectionParams}
import org.locationtech.geomesa.kafka08.KafkaDataStoreSchemaManager
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.tools.utils.KeywordParamSplitter

import scala.collection.JavaConversions._

class KafkaKeywordsCommand extends KafkaDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "keywords"
  override val params = new KafkaKeywordsParams()

  override def execute(): Unit = withDataStore { (ds) =>
    val sft = ds.getSchema(params.featureName)

    var keywordsModified = false

    if (params.removeAll) {
      val confirm = System.console().readLine("Remove all keywords? (y/n): ").toLowerCase
      if (confirm.equals("y") || confirm.equals("yes")) {
        sft.removeAllKeywords()
        keywordsModified = true
      } else {
        Command.user.info("Aborting operation")
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
      Command.output.info("Keywords: " + reloadedSft.getKeywords.mkString(", "))
    }

    ds.dispose()
  }
}

@Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
class KafkaKeywordsParams extends ProducerKDSConnectionParams with RequiredTypeNameParam {

  @Parameter(names = Array("-a", "--add"), description = "A keyword to add. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
  var keywordsToAdd: java.util.List[String] = null

  @Parameter(names = Array("-r", "--remove"), description = "A keyword to remove. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
  var keywordsToRemove: java.util.List[String] = null

  @Parameter(names = Array("-l", "--list"), description = "List all keywords on the schema")
  var list: Boolean = false

  @Parameter(names = Array("--removeAll"), description = "Remove all keywords on the schema")
  var removeAll: Boolean = false

}
