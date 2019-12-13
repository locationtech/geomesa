/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.AbstractAttributeIndex;
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex;
import com.googlecode.cqengine.persistence.support.ObjectSet;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.geomesa.memory.cqengine.query.Intersects;
import org.locationtech.geomesa.utils.index.BucketIndex;
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex;
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex$;
import org.locationtech.geomesa.utils.index.SpatialIndex;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GeoIndex<A extends Geometry, O extends SimpleFeature> extends AbstractAttributeIndex<A, O> implements OnHeapTypeIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeoIndex.class);

    private static final int INDEX_RETRIEVAL_COST = 40;

    SpatialIndex<SimpleFeature> index;
    int geomAttributeIndex;

    static Set<Class<? extends Query>> supportedQueries = new HashSet<Class<? extends Query>>() {{
        add(Intersects.class);
    }};

    public GeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute, int xBuckets, int yBuckets) {
        super(attribute, supportedQueries);
        geomAttributeIndex = sft.indexOf(attribute.getAttributeName());
        if (sft.getDescriptor(geomAttributeIndex).getType().getBinding() == Point.class) {
            index = new BucketIndex<>(xBuckets, yBuckets, new Envelope(-180.0, 180.0, -90.0, 90.0));
        } else {
            index = new SizeSeparatedBucketIndex<>(SizeSeparatedBucketIndex$.MODULE$.DefaultTiers(),
                                                   xBuckets / 360d,
                                                   yBuckets / 180d,
                                                   new Envelope(-180.0, 180.0, -90.0, 90.0));
        }
    }

    public static <A extends Geometry, O extends SimpleFeature> GeoIndex<A , O> onAttribute(SimpleFeatureType sft, Attribute<O, A> attribute) {
        return new GeoIndex<A, O>(sft, attribute, 360, 180);
    }

    public static <A extends Geometry, O extends SimpleFeature> GeoIndex<A , O> onAttribute(SimpleFeatureType sft, Attribute<O, A> attribute, int xBuckets, int yBuckets) {
        return new GeoIndex<A, O>(sft, attribute, xBuckets, yBuckets);
    }

    @Override
    public void init(ObjectStore<O> objectStore, QueryOptions queryOptions) {
        addAll(ObjectSet.fromObjectStore(objectStore, queryOptions), queryOptions);
    }

    @Override
    public boolean addAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        try {
            boolean modified = false;

            for (O object : objectSet) {
                Envelope env = ((Geometry)object.getDefaultGeometry()).getEnvelopeInternal();
                index.insert(env, object.getID(), object);
                modified = true;
            }

            return modified;
        }
        finally {
            objectSet.close();
        }
    }

    @Override
    public boolean removeAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        try {
            boolean modified = false;

            for (O object : objectSet) {
                Envelope env = ((Geometry)object.getDefaultGeometry()).getEnvelopeInternal();
                index.remove(env, object.getID());
                modified = true;
            }

            return modified;
        }
        finally {
            objectSet.close();
        }
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        this.index.clear();
    }

    @Override
    public ResultSet<O> retrieve(final Query<O> query, final QueryOptions queryOptions) {
        return new ResultSet<O>() {
            @Override
            public Iterator<O> iterator() {
                scala.collection.Iterator<SimpleFeature> iter =
                      getSimpleFeatureIteratorInternal((Intersects) query, queryOptions);
                return (Iterator<O>) JavaConversions.asJavaIterator(iter);
            }

            @Override
            public boolean contains(O object) {
                final Intersects intersects = (Intersects) query;
                Geometry geom = (Geometry) object.getAttribute(geomAttributeIndex);
                return intersects.matchesValue(geom, queryOptions);
            }

            @Override
            public boolean matches(O object) {
                return query.matches(object, queryOptions);
            }

            @Override
            public Query<O> getQuery() {
                return query;
            }

            @Override
            public QueryOptions getQueryOptions() {
                return queryOptions;
            }

            @Override
            public int getRetrievalCost() {
                return INDEX_RETRIEVAL_COST;
            }

            // Returning the size here as the MergeCost.
            // The geoindex size isn't optimal, so there might be a better
            // measure of this.
            @Override
            public int getMergeCost() {
                return size();
            }

            @Override
            public int size() {
                return getSimpleFeatureIteratorInternal((Intersects) query, queryOptions).size();
            }

            @Override
            public void close() {
            }
        };
    }

    private scala.collection.Iterator<SimpleFeature> getSimpleFeatureIteratorInternal(Intersects query,
                                                                                      final QueryOptions queryOptions) {
        final Intersects intersects = query;
        Envelope queryEnvelope = intersects.getEnvelope();
        return index.query(queryEnvelope).filter(new AbstractFunction1<SimpleFeature, Object>() {
            @Override
            public Object apply(SimpleFeature feature) {
                try {
                    Geometry geom = (Geometry) feature.getAttribute(geomAttributeIndex);
                    return intersects.matchesValue(geom, queryOptions);
                } catch (Exception e) {
                    LOGGER.warn("Caught exception while trying to look up geometry", e);
                    return false;
                }
            }
        });
    }


    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean isQuantized() {
        return false;
    }

    @Override
    public Index<O> getEffectiveIndex() {
        return this;
    }
}
