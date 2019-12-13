/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.nativeapi;

import org.geotools.factory.CommonFactoryFinder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.api.GeoMesaQuery;
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

    private GeoMesaQuery.GeoMesaQueryBuilder getQueryBuilder() {
        return GeoMesaQuery.GeoMesaQueryBuilder.builder()
                .within(-79.0, 37.0, -77.0, 39.0);
    }

    private Filter getDuringFilter(Date start, boolean includeStart, Date end, boolean includeEnd) {
        // create a query from these temporal parameters
        return getQueryBuilder()
                .during(start, includeStart, end, includeEnd)
                .getTemporalFilter();
    }

    private Filter getBeforeAfterFilter(Date start, Date end) {
        // create a query from these temporal parameters
        return getQueryBuilder()
                .before(end)
                .after(start)
                .getTemporalFilter();
    }

    private Filter getAfterFilter(Date date) {
        // create a query from these temporal parameters
        return getQueryBuilder()
                .after(date)
                .getTemporalFilter();
    }

    private Filter getBeforeFilter(Date date) {
        // create a query from these temporal parameters
        return getQueryBuilder()
                .before(date)
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
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(NO_DTG, false, NO_DTG, false));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(NO_DTG, false, NO_DTG, true));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(NO_DTG, true, NO_DTG, false));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(NO_DTG, true, NO_DTG, true));
    }

    @Test
    public void testAfter() {
        Assert.assertEquals(after(DTG_0_POST), getDuringFilter(DTG_0, false, NO_DTG, false));
        Assert.assertEquals(after(DTG_0_POST), getDuringFilter(DTG_0, false, NO_DTG, true));
        Assert.assertEquals(after(DTG_0), getDuringFilter(DTG_0, true, NO_DTG, false));
        Assert.assertEquals(after(DTG_0), getDuringFilter(DTG_0, true, NO_DTG, true));

        Assert.assertEquals(after(DTG_0_POST), getAfterFilter(DTG_0));
    }

    @Test
    public void testBefore() {
        Assert.assertEquals(before(DTG_1_PRE), getDuringFilter(NO_DTG, false, DTG_1, false));
        Assert.assertEquals(before(DTG_1), getDuringFilter(NO_DTG, false, DTG_1, true));
        Assert.assertEquals(before(DTG_1_PRE), getDuringFilter(NO_DTG, true, DTG_1, false));
        Assert.assertEquals(before(DTG_1), getDuringFilter(NO_DTG, true, DTG_1, true));

        Assert.assertEquals(before(DTG_0), getBeforeFilter(DTG_0_POST));
    }

    @Test
    public void testBetween() {
        Assert.assertEquals(between(DTG_0_POST, DTG_1_PRE), getDuringFilter(DTG_0, false, DTG_1, false));
        Assert.assertEquals(between(DTG_0_POST, DTG_1), getDuringFilter(DTG_0, false, DTG_1, true));
        Assert.assertEquals(between(DTG_0, DTG_1_PRE), getDuringFilter(DTG_0, true, DTG_1, false));
        Assert.assertEquals(between(DTG_0, DTG_1), getDuringFilter(DTG_0, true, DTG_1, true));

        Assert.assertEquals(between(DTG_0_POST, DTG_1_PRE), getBeforeAfterFilter(DTG_0, DTG_1));
    }

    @Test
    public void testInvalidTimeRange() {
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_0, false, DTG_0, false));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_0, false, DTG_0, true));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_0, true, DTG_0, false));
        Assert.assertNotEquals(Filter.INCLUDE, getDuringFilter(DTG_0, true, DTG_0, true));

        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_1, false, DTG_1, false));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_1, false, DTG_1, true));
        Assert.assertEquals(Filter.INCLUDE, getDuringFilter(DTG_1, true, DTG_1, false));
        Assert.assertNotEquals(Filter.INCLUDE, getDuringFilter(DTG_1, true, DTG_1, true));
    }

    @Test
    public void testAllTime() {
        Assert.assertEquals(Filter.INCLUDE,
                getQueryBuilder().during(DTG_0, false, DTG_1, false).allTime().getTemporalFilter());
        Assert.assertEquals(Filter.INCLUDE,
                getQueryBuilder().during(DTG_0, false, DTG_1, true).allTime().getTemporalFilter());
        Assert.assertEquals(Filter.INCLUDE,
                getQueryBuilder().during(DTG_0, true, DTG_1, false).allTime().getTemporalFilter());
        Assert.assertEquals(Filter.INCLUDE,
                getQueryBuilder().during(DTG_0, true, DTG_1, true).allTime().getTemporalFilter());

        Assert.assertEquals(Filter.INCLUDE, getQueryBuilder().after(DTG_0).allTime().getTemporalFilter());

        Assert.assertEquals(Filter.INCLUDE, getQueryBuilder().before(DTG_0).allTime().getTemporalFilter());
    }

    @Test
    public void testCompositionOverwrites() {
        // before(a).during(b,c) -> during(b,c)
        Assert.assertEquals(
                getQueryBuilder().before(DTG_1_PRE).during(DTG_0, true, DTG_1, true).getTemporalFilter(),
                getQueryBuilder().during(DTG_0, true, DTG_1, true).getTemporalFilter()
        );

        // during(a,b).before(c) -> during(a,c)
        Assert.assertEquals(
                getQueryBuilder().during(DTG_0, true, DTG_1, true).before(DTG_1).getTemporalFilter(),
                getQueryBuilder().during(DTG_0, true, DTG_1_PRE, true).getTemporalFilter()
        );

        // after(a).during(b,c) -> during(b,c)
        Assert.assertEquals(
                getQueryBuilder().after(DTG_1_PRE).during(DTG_0, true, DTG_1, true).getTemporalFilter(),
                getQueryBuilder().during(DTG_0, true, DTG_1, true).getTemporalFilter()
        );

        // during(a,b).after(c) -> during(a,c)
        Assert.assertEquals(
                getQueryBuilder().during(DTG_0, true, DTG_1, true).after(DTG_0).getTemporalFilter(),
                getQueryBuilder().during(DTG_0_POST, true, DTG_1, true).getTemporalFilter()
        );

        // before(a).before(b) -> before(b)
        Assert.assertEquals(
                getQueryBuilder().before(DTG_0).before(DTG_1).getTemporalFilter(),
                getQueryBuilder().before(DTG_1).getTemporalFilter()
        );

        // after(a).after(b) -> after(b)
        Assert.assertEquals(
                getQueryBuilder().after(DTG_0).after(DTG_1).getTemporalFilter(),
                getQueryBuilder().after(DTG_1).getTemporalFilter()
        );
    }
}
