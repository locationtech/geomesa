package org.locationtech.geomesa.accumulo.index

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.joda.time.Interval

trait IndexFilterHelpers {

   def buildFilter(geom: Geometry, interval: Interval): KeyPlanningFilter =
     (IndexSchema.somewhere(geom), IndexSchema.somewhen(interval)) match {
       case (None, None)       =>    AcceptEverythingFilter
       case (None, Some(i))    =>
         if (i.getStart == i.getEnd) DateFilter(i.getStart)
         else                        DateRangeFilter(i.getStart, i.getEnd)
       case (Some(p), None)    =>    SpatialFilter(p)
       case (Some(p), Some(i)) =>
         if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
         else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
     }

   def netGeom(geom: Geometry): Geometry =
     Option(geom).map(_.intersection(IndexSchema.everywhere)).orNull

   def netInterval(interval: Interval): Interval = interval match {
     case null => null
     case _    => IndexSchema.everywhen.overlap(interval)
   }

   def netPolygon(poly: Polygon): Polygon = poly match {
     case null => null
     case p if p.covers(IndexSchema.everywhere) =>
       IndexSchema.everywhere
     case p if IndexSchema.everywhere.covers(p) => p
     case _ => poly.intersection(IndexSchema.everywhere).
       asInstanceOf[Polygon]
   }

 }
