/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools._
import org.opengis.filter.Filter

trait DeleteFeaturesCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] {

  override val name = "delete-features"
  override def params: DeleteFeaturesParams

  override def execute(): Unit = {
    val sftName = params.featureName
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    Command.user.info(s"Deleting features from $sftName with filter $filter. This may take a few moments...")
    withDataStore(_.getFeatureSource(sftName).removeFeatures(filter))
    Command.user.info("Features deleted")
  }
}

// @Parameters(commandDescription = "Delete features from a table in GeoMesa. Does not delete any tables or schema information.")
trait DeleteFeaturesParams extends CatalogParam with RequiredTypeNameParam with OptionalCqlFilterParam
