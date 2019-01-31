/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.status

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.{ArrowDataStoreCommand, UrlParam}
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand
import org.opengis.feature.simple.SimpleFeatureType

class ArrowDescribeSchemaCommand extends DescribeSchemaCommand[ArrowDataStore] with ArrowDataStoreCommand {
  override val params = new ArrowDescribeSchemaParams

  override protected def getSchema(ds: ArrowDataStore): SimpleFeatureType = ds.getSchema

  override protected def describe(ds: ArrowDataStore, sft: SimpleFeatureType, output: String => Unit): Unit = {
    super.describe(ds, sft, output)
    output("")
    val dictionaries = ds.dictionaries
    if (dictionaries.isEmpty) {
      output("Dictionaries: none")
    } else if (params.dictionaries) {
      output("Dictionaries:")
      dictionaries.foreach { case (field, dictionary) => output(s"  $field: ${dictionary.iterator.mkString(", ")}") }
    } else {
      output(s"Dictionaries: ${ds.dictionaries.keys.mkString(", ")}")
    }
  }
}

@Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
class ArrowDescribeSchemaParams extends UrlParam {
  @Parameter(names = Array("--show-dictionaries"), description = "Show dictionary values")
  var dictionaries: Boolean = false
}
