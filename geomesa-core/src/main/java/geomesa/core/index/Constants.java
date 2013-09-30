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

package geomesa.core.index;

import org.joda.time.DateTime;

/**
 * This is a utility class to allow for simpler use of Scala constants within
 * the Java portion of the API.
 */
public class Constants {
    public final static String SF_PROPERTY_GEOMETRY =
            geomesa.core.index.package$.MODULE$.SF_PROPERTY_GEOMETRY();
    public final static String SF_PROPERTY_START_TIME =
            geomesa.core.index.package$.MODULE$.SF_PROPERTY_START_TIME();
    public final static String SF_PROPERTY_END_TIME =
            geomesa.core.index.package$.MODULE$.SF_PROPERTY_END_TIME();

    public final static DateTime MIN_DATE = geomesa.core.index.package$.MODULE$.MIN_DATE();
    public final static DateTime MAX_DATE = geomesa.core.index.package$.MODULE$.MAX_DATE();
}
