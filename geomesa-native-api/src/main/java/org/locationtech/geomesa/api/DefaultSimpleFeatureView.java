/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.api;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.classification.InterfaceStability;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.geotools.SftBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

@InterfaceStability.Unstable
public class DefaultSimpleFeatureView<T> implements SimpleFeatureView<T> {

    private IndexType indexType;

    private Logger logger = LoggerFactory.getLogger(DefaultSimpleFeatureView.class);
    private SimpleFeatureType sft;
    private ObjectPool<SimpleFeatureBuilder> builderPool;

    public static DefaultSimpleFeatureView spatialView() {
        return new DefaultSimpleFeatureView(IndexType.SPATIAL);
    }

    public static DefaultSimpleFeatureView spatioTemporalView() {
        return new DefaultSimpleFeatureView(IndexType.SPATIOTEMPORAL);
    }

    private DefaultSimpleFeatureView(IndexType indexType) {
        this.indexType = indexType;
        if (indexType == IndexType.SPATIAL) {
            sft = new SftBuilder().point("geom", true).build("spatialidx");
        } else if (indexType == IndexType.SPATIOTEMPORAL) {
            sft = new SftBuilder().point("geom", true).date("dtg", true).build("spatiotemporalidx");
        }

        builderPool = new GenericObjectPool<>(new BasePooledObjectFactory<SimpleFeatureBuilder>() {
            @Override
            public SimpleFeatureBuilder create() throws Exception {
                return new SimpleFeatureBuilder(sft);
            }

            @Override
            public PooledObject<SimpleFeatureBuilder> wrap(SimpleFeatureBuilder obj) {
                return new DefaultPooledObject<>(obj);
            }
        });
    }

    @Override
    public IndexType indexType() {
        return indexType;
    }

    public SimpleFeatureType buildSimpleFeatureType() {
        return sft;
    }

    @Override
    public SimpleFeature buildSimpleFeature(T t, Geometry geometry) {
        Preconditions.checkNotNull(geometry, "Geometry cannot be null");
        SimpleFeatureBuilder builder = null;
        SimpleFeature ret = null;
        try {
            builder = builderPool.borrowObject();
            builder.reset();
            builder.add(geometry);
            ret = builder.buildFeature(UUID.randomUUID().toString());
        } catch (Exception e) {
            logger.error("Could not get builder", e);
        } finally {
            if (builder != null) {
                try {
                    builderPool.returnObject(builder);
                } catch (Exception e) {
                    logger.error("Could not return builder", e);
                }
            }
        }
        return ret;
    }

    @Override
    public SimpleFeature buildSimpleFeature(T t, Geometry geometry, Date date) {
        Preconditions.checkNotNull(geometry, "Geometry cannot be null");
        SimpleFeatureBuilder builder = null;
        SimpleFeature ret = null;
        try {
            builder = builderPool.borrowObject();
            builder.reset();
            builder.add(geometry);
            builder.add(date);
            ret = builder.buildFeature(UUID.randomUUID().toString());
        } catch (Exception e) {
            logger.error("Could not get builder", e);
        } finally {
            if (builder != null) {
                try {
                    builderPool.returnObject(builder);
                } catch (Exception e) {
                    logger.error("Could not return builder", e);
                }
            }
        }
        return ret;
    }


}