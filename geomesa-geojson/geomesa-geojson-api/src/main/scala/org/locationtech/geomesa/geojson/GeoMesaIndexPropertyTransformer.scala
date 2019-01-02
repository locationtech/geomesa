/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson

import org.locationtech.geomesa.features.kryo.json.JsonPathParser
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathElement}
import org.locationtech.geomesa.geojson.query.PropertyTransformer

class GeoMesaIndexPropertyTransformer(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]])
    extends PropertyTransformer {

  // noinspection ExistsEquals
  override def useFid(prop: String): Boolean = idPath.exists(_ == JsonPathParser.parse(prop))

  override def transform(prop: String): String = {
    JsonPathParser.parse(prop) match {
      case Seq(PathAttribute("geometry", _)) => "geom"
      case `dtgPath` => "dtg"
      case x => JsonPathParser.print(PathAttribute("json") +: x)
    }
  }
}
