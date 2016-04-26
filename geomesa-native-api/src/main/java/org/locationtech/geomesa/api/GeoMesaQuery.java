package org.locationtech.geomesa.api;

import org.geotools.factory.CommonFactoryFinder;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.util.Date;

/**
 * Represents a query to GeoMesa
 */
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
