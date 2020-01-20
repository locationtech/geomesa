/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index.param;

import org.locationtech.geomesa.memory.cqengine.index.GeoIndexType;

public class BucketIndexParam implements GeoIndexParams {
    private int xBuckets = 360;
    private int yBuckets = 180;


    public BucketIndexParam() {
    }

    public BucketIndexParam(int xBuckets, int yBuckets) {
        this.xBuckets = xBuckets;
        this.yBuckets = yBuckets;
    }

    public int getxBuckets() {
        return xBuckets;
    }

    public int getyBuckets() {
        return yBuckets;
    }

    @Override
    public GeoIndexType getGeoIndexType() {
        return GeoIndexType.Bucket;
    }
}
