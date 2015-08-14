/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

object AvailableTables {
  val AllTables: List[GeoMesaTable] = List(AttributeTable, RecordTable, Z3Table, SpatioTemporalTable)
  val AllTablesStr: List[String] = List(AttributeTable, RecordTable, Z3Table, SpatioTemporalTable).map(_.suffix)
  val DefaultTables: List[GeoMesaTable] = AllTables
  val DefaultTablesStr: List[String] = AllTables.map(_.suffix)
  val Z3TableSchemeStr: List[String] = List(AttributeTable, RecordTable, Z3Table).map(_.suffix)
  
  def toTables(sList: List[String]) = sList.flatMap( s => AvailableTables.AllTables.find(_.suffix == s))
  
}