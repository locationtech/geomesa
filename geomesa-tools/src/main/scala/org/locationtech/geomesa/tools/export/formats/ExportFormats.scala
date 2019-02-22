/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

object ExportFormats extends Enumeration {
  type ExportFormat = Value
  val Arrow, Avro, Bin, Csv, GeoJson, Gml, Html, Json, Leaflet, Null, Orc, Parquet, Shp, Tsv, Xml = Value
}
