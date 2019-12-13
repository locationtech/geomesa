/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api;

import org.apache.hadoop.classification.InterfaceStability;
import org.geotools.factory.CommonFactoryFinder;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;

import java.util.Date;

/**
 * Represents a query to GeoMesa
 */
@InterfaceStability.Unstable
@Deprecated
public class GeoMesaQuery {

    private Filter filter = Filter.INCLUDE;

    public static class GeoMesaQueryBuilder {
        private static final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        private static PropertyName DTGProperty = ff.property("dtg");
        private Double minx, miny, maxx, maxy;
        private Date start;
        private Date end;
        private Filter extraFilter = Filter.INCLUDE;

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

        public GeoMesaQueryBuilder before(Date end) {
            return during(null, false, end, false);
        }

        public GeoMesaQueryBuilder after(Date start) {
            return during(start, false, null, false);
        }

        public GeoMesaQueryBuilder allTime() {
            this.start = null;
            this.end = null;
            return this;
        }

        public GeoMesaQueryBuilder during(Date start, Date end) {
            return during(start, true, end, true);
        }

        /**
         * We implement the "during" as a GeoTools "between" predicate, which includes the endpoints
         * by default; see:
         *   https://github.com/geotools/geotools/blob/15.x/modules/library/main/src/main/java/org/geotools/filter/IsBetweenImpl.java#L144
         *
         * If the temporal bounds are set to non-sensical values, that will be handled elsewhere.
         *
         * WARNING:  NULL values do *NOT* overwrite existing start/end values for the query builder.
         * To clear out the start/end values, you must use the "allTime" method.
         *
         * @param start the start date
         * @param isStartInclusive whether the start date should be included in the interval
         * @param end the end date
         * @param isEndInclusive whether the end date should be included in the interval
         * @return the query object updated for the effective temporal bounds
         */
        public GeoMesaQueryBuilder during(Date start, boolean isStartInclusive, Date end, boolean isEndInclusive) {
            if (start != null) {
                if (isStartInclusive) {
                    this.start = start;
                } else {
                    this.start = new Date(start.getTime() + 1L);
                }
            }

            if (end != null) {
                if (isEndInclusive) {
                    this.end = end;
                } else {
                    this.end = new Date(end.getTime() - 1L);
                }
            }

            return this;
        }

        /**
         * Converts the current start- and end-point into a valid temporal filter.
         *
         * @return the net temporal filter that results from the current temporal bounds
         */
        public Filter getTemporalFilter() {
            // if neither end-point is set, there is no valid filter
            if (start == null && end == null) return Filter.INCLUDE;

            // if the start-point alone is defined, this is an AFTER query
            if (end == null) return ff.after(DTGProperty, ff.literal(start));

            // if the end-point alone is defined, this is a BEFORE query
            if (start == null) return ff.before(DTGProperty, ff.literal(end));

            // both are defined; if they are ordered correctly, this is a DURING query
            if (start.getTime() <= end.getTime()) return ff.between(DTGProperty, ff.literal(start), ff.literal(end));

            // if you get this far, you have non-null temporal bounds that are improperly ordered;
            // this is equivalent to having no valid temporal bounds
            // TODO:  log this occurrence, because it is unexpected, and the developer should be informed
            return Filter.INCLUDE;
        }

        public GeoMesaQueryBuilder filter(Filter filter) {
            extraFilter = ff.and(extraFilter, filter);
            return this;
        }

        public GeoMesaQuery build() {
            // TODO: need to respect the SimpleFeatureView
            GeoMesaQuery query = new GeoMesaQuery();
            BBOX geoFilter = ff.bbox("geom", minx, miny, maxx, maxy, "EPSG:4326");
            query.filter = ff.and(extraFilter, ff.and(geoFilter, getTemporalFilter()));
            return query;
        }
    }

    public static GeoMesaQuery include() {
        GeoMesaQuery query = new GeoMesaQuery();
        query.filter = Filter.INCLUDE;
        return query;
    }

    public Filter getFilter() {
        return filter;
    }
}
