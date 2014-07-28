/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.iterators;

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