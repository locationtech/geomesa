/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.tools._
import org.opengis.filter.Filter

trait ExplainCommand[DS <: GeoMesaDataStore[DS]] extends DataStoreCommand[DS] {

  override def params: ExplainParams

  override val name: String = "explain"

  override def execute(): Unit = withDataStore(explain)

  protected def explain(ds: DS): Unit = {
    val query = new Query(params.featureName, Option(params.cqlFilter).getOrElse(Filter.INCLUDE))
    Option(params.attributes).filterNot(_.isEmpty).foreach(query.setPropertyNames)
    Option(params.hints).foreach { hints =>
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
      ViewParams.setHints(query)
    }
    Option(params.index).foreach { index =>
      Command.user.debug(s"Using index $index")
      query.getHints.put(QueryHints.QUERY_INDEX, index)
    }
    val explainString = new ExplainString()
    ds.getQueryPlan(query, None, explainString)
    Command.output.info(explainString.toString)
  }
}

@Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
class ExplainParams extends QueryParams with RequiredCqlFilterParam with QueryHintsParams with OptionalIndexParam
