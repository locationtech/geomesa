/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.ExplainPrintln
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait ExplainCommand[DS <: GeoMesaDataStore[DS, _, _, _]] extends DataStoreCommand[DS] {

  override def params: ExplainParams

  override val name: String = "explain"

  override def execute(): Unit = {
    try {
      withDataStore(explain)
    } catch {
      case NonFatal(e) => logger.error("Could not get explain plan:", e)
    }
  }

  protected def explain(ds: DS): Unit = {
    val query = new Query(params.featureName, Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE))
    Option(params.attributes).filterNot(_.isEmpty).foreach(query.setPropertyNames)
    params.loadIndex(ds, IndexMode.Read).foreach { index =>
      query.getHints.put(QueryHints.QUERY_INDEX_KEY, index)
      logger.debug(s"Using index ${index.identifier}")
    }
    ds.getQueryPlan(query, None, new ExplainPrintln(System.err))
  }
}

@Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
class ExplainParams extends QueryParams with RequiredCqlFilterParam with OptionalIndexParam
