package org.locationtech.geomesa.api;

import org.geotools.factory.CommonFactoryFinder;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;

import java.util.Date;

public class GeoMesaQueryTest {
    private static final Date DTG_0 = new Date(116, 0, 1);
    private static final Date DTG_0_POST = new Date(DTG_0.getTime() + 1L);
    private static final Date DTG_1 = new Date(116, 2, 1);
    private static final Date DTG_1_PRE = new Date(DTG_1.getTime() - 1L);
    private static final Date NO_DTG = null;

    private static final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    private static PropertyName DTGProperty = ff.property("dtg");

    private Filter getTemporalFilter(Date start, boolean includeStart, Date end, boolean includeEnd) {
        // create a query from these temporal parameters
        return GeoMesaQuery.GeoMesaQueryBuilder.builder()
                .within(-79.0, 37.0, -77.0, 39.0)
                .during(start, includeStart, end, includeEnd)
                .getTemporalFilter();
    }

    private Filter after(Date date) {
        return ff.after(DTGProperty, ff.literal(date));
    }

    private Filter before(Date date) {
        return ff.before(DTGProperty, ff.literal(date));
    }

    private Filter between(Date start, Date end) {
        return ff.between(DTGProperty, ff.literal(start), ff.literal(end));
    }

    @Test
    public void testNoTimeRange() {
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(NO_DTG, false, NO_DTG, false));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(NO_DTG, false, NO_DTG, true));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(NO_DTG, true, NO_DTG, false));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(NO_DTG, true, NO_DTG, true));
    }

    @Test
    public void testAfter() {
        Assert.assertEquals(after(DTG_0_POST), getTemporalFilter(DTG_0, false, NO_DTG, false));
        Assert.assertEquals(after(DTG_0_POST), getTemporalFilter(DTG_0, false, NO_DTG, true));
        Assert.assertEquals(after(DTG_0), getTemporalFilter(DTG_0, true, NO_DTG, false));
        Assert.assertEquals(after(DTG_0), getTemporalFilter(DTG_0, true, NO_DTG, true));
    }

    @Test
    public void testBefore() {
        Assert.assertEquals(before(DTG_1_PRE), getTemporalFilter(NO_DTG, false, DTG_1, false));
        Assert.assertEquals(before(DTG_1), getTemporalFilter(NO_DTG, false, DTG_1, true));
        Assert.assertEquals(before(DTG_1_PRE), getTemporalFilter(NO_DTG, true, DTG_1, false));
        Assert.assertEquals(before(DTG_1), getTemporalFilter(NO_DTG, true, DTG_1, true));
    }

    @Test
    public void testBetween() {
        Assert.assertEquals(between(DTG_0_POST, DTG_1_PRE), getTemporalFilter(DTG_0, false, DTG_1, false));
        Assert.assertEquals(between(DTG_0_POST, DTG_1), getTemporalFilter(DTG_0, false, DTG_1, true));
        Assert.assertEquals(between(DTG_0, DTG_1_PRE), getTemporalFilter(DTG_0, true, DTG_1, false));
        Assert.assertEquals(between(DTG_0, DTG_1), getTemporalFilter(DTG_0, true, DTG_1, true));
    }

    @Test
    public void testInvalidTimeRange() {
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_0, false, DTG_0, false));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_0, false, DTG_0, true));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_0, true, DTG_0, false));
        Assert.assertNotEquals(Filter.INCLUDE, getTemporalFilter(DTG_0, true, DTG_0, true));

        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_1, false, DTG_1, false));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_1, false, DTG_1, true));
        Assert.assertEquals(Filter.INCLUDE, getTemporalFilter(DTG_1, true, DTG_1, false));
        Assert.assertNotEquals(Filter.INCLUDE, getTemporalFilter(DTG_1, true, DTG_1, true));
    }
}
