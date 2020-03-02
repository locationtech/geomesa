/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.memory.cqengine.query.Intersects;
import org.locationtech.geomesa.utils.index.SpatialIndex;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractGeoIndex<A extends Geometry, O extends SimpleFeature>
      extends AbstractAttributeIndex<A, O> implements OnHeapTypeIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoIndex.class);

    private static final int INDEX_RETRIEVAL_COST = 40;

    private final SpatialIndex<O> index;
    private final int geomAttributeIndex;

    public static final ThreadLocal<SpatialIndex<? extends SimpleFeature>> lastUsed = new ThreadLocal<>();

    static Set<Class<? extends Query>> supportedQueries = new HashSet<Class<? extends Query>>() {{
        add(Intersects.class);
    }};

    AbstractGeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute, SpatialIndex<O> index) {
        super(attribute, supportedQueries);
        this.index = index;
        this.geomAttributeIndex = sft.indexOf(attribute.getAttributeName());
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
                Geometry geom = (Geometry) object.getDefaultGeometry();
                index.insert(geom, object.getID(), object);
                modified = true;
            }

            return modified;
        } finally {
            objectSet.close();
        }
    }

    @Override
    public boolean removeAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        try {
            boolean modified = false;

            for (O object : objectSet) {
                Geometry geom = (Geometry) object.getDefaultGeometry();
                index.remove(geom, object.getID());
                modified = true;
            }

            return modified;
        } finally {
            objectSet.close();
        }
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        this.index.clear();
    }

    @Override
    public ResultSet<O> retrieve(final Query<O> query, final QueryOptions queryOptions) {
        lastUsed.set(this.index);
        return new GeoIndexResultSet(query, queryOptions);
    }

    private scala.collection.Iterator<O> getSimpleFeatureIteratorInternal(Intersects query,
                                                                          final QueryOptions queryOptions) {
        final Intersects intersects = query;
        Envelope queryEnvelope = intersects.getEnvelope();
        return index.query(queryEnvelope).filter((Function1<O, Object>) new AbstractFunction1<SimpleFeature, Object>() {
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

    public class GeoIndexResultSet extends ResultSet<O> {

        private final Query<O> query;
        private final QueryOptions queryOptions;

        public GeoIndexResultSet(Query<O> query, QueryOptions queryOptions) {
            this.query = query;
            this.queryOptions = queryOptions;
        }

        @Override
        public Iterator<O> iterator() {
            scala.collection.Iterator<O> iter =
                  getSimpleFeatureIteratorInternal((Intersects) query, queryOptions);
            return JavaConversions.asJavaIterator(iter);
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
    }
}
