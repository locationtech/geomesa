/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.stats

import java.util.Date

import org.apache.accumulo.core.client.{Connector, Scanner}
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations

/**
 * Abstract class for querying stats
 *
 * @param connector
 * @param statTableForFeatureName
 * @tparam S
 */
abstract class StatReader[S <: Stat](connector: Connector, statTableForFeatureName: (String) => String) {

  protected def statTransform: StatTransform[S]

  /**
   * Main query method based on date range. Subclasses can add additional query capability (on
   * attributes, for instance).
   *
   * @param featureName
   * @param start
   * @param end
   * @param authorizations
   * @return
   */
  def query(featureName: String, start: Date, end: Date, authorizations: Authorizations): Iterator[S] = {
    val table = statTableForFeatureName(featureName)

    val scanner = connector.createScanner(table, authorizations)
    val rangeStart = s"$featureName~${StatTransform.dateFormat.print(start.getTime)}"
    val rangeEnd = s"$featureName~${StatTransform.dateFormat.print(end.getTime)}"
    scanner.setRange(new AccRange(rangeStart, rangeEnd))

    configureScanner(scanner)

    statTransform.iterator(scanner)
  }

  /**
   * Can be implemented by subclasses to configure scans beyond a simple date range
   *
   * @param scanner
   */
  protected def configureScanner(scanner: Scanner)
}

/**
 * Class for querying query stats
 *
 * @param connector
 * @param statTableForFeatureName
 */
class QueryStatReader(connector: Connector, statTableForFeatureName: String => String)
    extends StatReader[QueryStat](connector, statTableForFeatureName) {

  override protected val statTransform = QueryStatTransform

  override protected def configureScanner(scanner: Scanner) = {}
}
