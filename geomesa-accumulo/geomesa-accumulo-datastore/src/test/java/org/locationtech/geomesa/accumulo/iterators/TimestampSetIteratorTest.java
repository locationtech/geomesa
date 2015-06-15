/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators;

import junit.framework.Assert;
import org.junit.Test;

public class TimestampSetIteratorTest {
    @Test
    public void createIteratorByReflection() throws Exception {
        Class clazz = (new TimestampSetIterator()).getClass();
        Assert.assertNotNull(
                "Could not instantiate TimestampSetIterator via parameterless constructor",
                clazz);

        Object obj = clazz.newInstance();
        Assert.assertNotNull(
                "Could not create new instance of TimestampSetIterator",
                obj);

        TimestampSetIterator tsi = (TimestampSetIterator)obj;
        Assert.assertNotNull(
                "Could not cast new instance of TimestampSetIterator to the proper type",
                tsi);
    }
}