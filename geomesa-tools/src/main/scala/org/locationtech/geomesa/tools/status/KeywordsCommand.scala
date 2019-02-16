/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools.utils.KeywordParamSplitter
import org.locationtech.geomesa.tools.{Command, DataStoreCommand, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

trait KeywordsCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name: String = "keywords"

  override def params: KeywordsParams

  override def execute(): Unit = withDataStore(modifyKeywords)

  protected def modifyKeywords(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    import scala.collection.JavaConversions._

    val sft = Option(ds.getSchema(params.featureName)).map(SimpleFeatureTypes.mutable).orNull
    if (sft == null) {
      throw new ParameterException(s"Feature '${params.featureName}' not found")
    }
    if (params.removeAll) {
      val confirm = System.console().readLine("Remove all keywords? (y/n): ").toLowerCase()
      if (confirm.equals("y") || confirm.equals("yes")) {
        sft.removeAllKeywords()
      } else {
        Command.user.info("Aborting operation")
        return
      }
    } else if (params.keywordsToRemove != null) {
      sft.removeKeywords(params.keywordsToRemove.toSet)
    }

    if (params.keywordsToAdd != null) {
      sft.addKeywords(params.keywordsToAdd.toSet)
    }

    ds.updateSchema(params.featureName, sft)

    if (params.list) {
      Command.output.info("Keywords: " + ds.getSchema(sft.getTypeName).getKeywords.mkString(", "))
    }
  }
}

@Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
trait KeywordsParams extends RequiredTypeNameParam {

  @Parameter(names = Array("-a", "--add"), description = "A keyword to add. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
  var keywordsToAdd: java.util.List[String] = _

  @Parameter(names = Array("-r", "--remove"), description = "A keyword to remove. Can be specified multiple times", splitter = classOf[KeywordParamSplitter])
  var keywordsToRemove: java.util.List[String] = _

  @Parameter(names = Array("-l", "--list"), description = "List all keywords on the schema")
  var list: Boolean = false

  @Parameter(names = Array("--removeAll"), description = "Remove all keywords on the schema")
  var removeAll: Boolean = false
}
