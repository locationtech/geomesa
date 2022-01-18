/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import org.geotools.data.DataStore
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.DeleteFeaturesCommand.DeleteFeaturesParams
import org.locationtech.geomesa.tools.utils.Prompt
import org.opengis.filter.Filter

trait DeleteFeaturesCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name = "delete-features"
  override def params: DeleteFeaturesParams

  override def execute(): Unit = {
    val sftName = params.featureName
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
    val msg = s"Deleting features from schema '$sftName' with filter: ${ECQL.toCQL(filter)}"
    if (params.force || Prompt.confirm(s"$msg\nContinue (y/n)? ")) {
      Command.user.info(s"Deleting features, please wait...")
      withDataStore { ds =>
        ds.getFeatureSource(sftName) match {
          case fs: SimpleFeatureStore => fs.removeFeatures(filter)
          case fs => throw new IllegalStateException(s"Expected SimpleFeatureStore, got ${Option(fs).map(_.getClass.getName).orNull}")
        }
      }
      Command.user.info("Features deleted")
    }
  }
}

object DeleteFeaturesCommand {
  // @Parameters(commandDescription = "Delete features from a table in GeoMesa. Does not delete any tables or schema information.")
  trait DeleteFeaturesParams extends RequiredTypeNameParam with OptionalCqlFilterParam with OptionalForceParam
}
