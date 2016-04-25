/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.visitor

import com.vividsolutions.jts.geom.Envelope
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
import org.opengis.filter._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

/**
  * Helper for extracting bounds from a filter. For some reason, ExtractBoundsFilterVisitor does
  * not accept a generic Filter...
  */
object BoundsFilterVisitor {

  /**
    * Extract the bounds from a filter.
    *
    * @param filter fitler to evaluate
    * @param envelope an existing envelope that will be included in the final bounds
    * @return the bounds
    */
  def visit(filter: Filter, envelope: Envelope = null): ReferencedEnvelope = {

    val visitor = ExtractBoundsFilterVisitor.BOUNDS_VISITOR

    val result = filter match {
      case f: ExcludeFilter                  => visitor.visit(f, envelope)
      case f: IncludeFilter                  => visitor.visit(f, envelope)
      case f: And                            => visitor.visit(f, envelope)
      case f: Id                             => visitor.visit(f, envelope)
      case f: Not                            => visitor.visit(f, envelope)
      case f: Or                             => visitor.visit(f, envelope)
      case f: PropertyIsBetween              => visitor.visit(f, envelope)
      case f: PropertyIsEqualTo              => visitor.visit(f, envelope)
      case f: PropertyIsNotEqualTo           => visitor.visit(f, envelope)
      case f: PropertyIsGreaterThan          => visitor.visit(f, envelope)
      case f: PropertyIsGreaterThanOrEqualTo => visitor.visit(f, envelope)
      case f: PropertyIsLessThan             => visitor.visit(f, envelope)
      case f: PropertyIsLessThanOrEqualTo    => visitor.visit(f, envelope)
      case f: PropertyIsLike                 => visitor.visit(f, envelope)
      case f: PropertyIsNull                 => visitor.visit(f, envelope)
      case f: PropertyIsNil                  => visitor.visit(f, envelope)
      case f: BBOX                           => visitor.visit(f, envelope)
      case f: Beyond                         => visitor.visit(f, envelope)
      case f: Contains                       => visitor.visit(f, envelope)
      case f: Crosses                        => visitor.visit(f, envelope)
      case f: Disjoint                       => visitor.visit(f, envelope)
      case f: DWithin                        => visitor.visit(f, envelope)
      case f: Equals                         => visitor.visit(f, envelope)
      case f: Intersects                     => visitor.visit(f, envelope)
      case f: Overlaps                       => visitor.visit(f, envelope)
      case f: Touches                        => visitor.visit(f, envelope)
      case f: Within                         => visitor.visit(f, envelope)
      case f: After                          => visitor.visit(f, envelope)
      case f: AnyInteracts                   => visitor.visit(f, envelope)
      case f: Before                         => visitor.visit(f, envelope)
      case f: Begins                         => visitor.visit(f, envelope)
      case f: BegunBy                        => visitor.visit(f, envelope)
      case f: During                         => visitor.visit(f, envelope)
      case f: EndedBy                        => visitor.visit(f, envelope)
      case f: Ends                           => visitor.visit(f, envelope)
      case f: Meets                          => visitor.visit(f, envelope)
      case f: MetBy                          => visitor.visit(f, envelope)
      case f: OverlappedBy                   => visitor.visit(f, envelope)
      case f: TContains                      => visitor.visit(f, envelope)
      case f: TEquals                        => visitor.visit(f, envelope)
      case f: TOverlaps                      => visitor.visit(f, envelope)
      case _                                 => visitor.visitNullFilter(envelope)
    }

    wholeWorldEnvelope.intersection(result.asInstanceOf[Envelope])
  }
}
