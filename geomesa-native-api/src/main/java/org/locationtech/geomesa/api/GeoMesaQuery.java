/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.api;

import org.apache.hadoop.classification.InterfaceStability;
import org.geotools.factory.CommonFactoryFinder;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.util.Date;

/**
 * Represents a query to GeoMesa
 */
@InterfaceStability.Unstable
public class GeoMesaQuery {

    private Filter filter = Filter.INCLUDE;

    public static class GeoMesaQueryBuilder {
        private static final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        private Double minx, miny, maxx, maxy;
        private Date start;
        private Date end;
        public static GeoMesaQueryBuilder builder() {
            return new GeoMesaQueryBuilder();
        }

        public GeoMesaQueryBuilder within(double lx, double ly, double ux, double uy) {
            minx = lx;
            miny = ly;
            maxx = ux;
            maxy = uy;
            return this;
        }

        public GeoMesaQueryBuilder during(Date start, Date end) {
            this.start = start;
            this.end = end;
            return this;
        }

        public GeoMesaQuery build() {
            GeoMesaQuery query = new GeoMesaQuery();
            query.filter = ff.and(
                    ff.bbox("geom", minx, maxx, miny, maxy, "EPSG:4326"),
                    ff.between(ff.property("dtg"), ff.literal(start), ff.literal(end)));
            return query;
        }
    }

    public Filter getFilter() {
        return filter;
    }
}
