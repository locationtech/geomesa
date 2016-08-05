/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.visitor

import com.vividsolutions.jts.geom.Envelope
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.HotfixExtractBoundsFilterVisitor
import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
import org.opengis.filter._

/**
  * Helper for extracting bounds from a filter
  */
object BoundsFilterVisitor {

  /**
    * Extract the bounds from a filter
    *
    * @param filter filter to evaluate
    * @param envelope an existing envelope that will be included in the final bounds
    * @return the bounds
    */
  def visit(filter: Filter, envelope: Envelope = null): ReferencedEnvelope = {
    // TODO GEOMESA-1350 for GeoMesa 1.3 release go back to GeoTools fixed version
    //val result = filter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, envelope).asInstanceOf[Envelope]
    val result = filter.accept(HotfixExtractBoundsFilterVisitor.BOUNDS_VISITOR, envelope).asInstanceOf[Envelope]
    if (result == null) {
      wholeWorldEnvelope
    } else {
      wholeWorldEnvelope.intersection(result)
    }
  }
}
