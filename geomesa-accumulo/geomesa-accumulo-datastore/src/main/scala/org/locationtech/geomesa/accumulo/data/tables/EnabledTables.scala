/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

trait EnabledTables {

  /**
   * Returns a list of available table storage types
   */
  def getEnabledTables: List[GeoMesaTable]
}

object EnabledTables {
  val AllTables: List[GeoMesaTable] = List(AttributeTable, RecordTable, Z3Table, SpatioTemporalTable)
  val DefaultTables: List[GeoMesaTable] = AllTables
  val DefaultTablesStr: List[String] = AllTables.map(_.suffix)
  val Z3TableScheme: List[GeoMesaTable] = List(AttributeTable, RecordTable, Z3Table)
}