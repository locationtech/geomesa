/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.index;

import org.joda.time.DateTime;

/**
 * This is a utility class to allow for simpler use of Scala constants within
 * the Java portion of the API.
 */
public class Constants {
  public final static String SF_PROPERTY_GEOMETRY   = package$.MODULE$.SF_PROPERTY_GEOMETRY();
  public final static String SF_PROPERTY_START_TIME = package$.MODULE$.SF_PROPERTY_START_TIME();
  public final static String SF_PROPERTY_END_TIME   = package$.MODULE$.SF_PROPERTY_END_TIME();
  public final static String SFT_INDEX_SCHEMA       = package$.MODULE$.SFT_INDEX_SCHEMA();

  public final static String TYPE_SPEC              = package$.MODULE$.spec();

  public final static DateTime MIN_DATE = package$.MODULE$.MIN_DATE();
  public final static DateTime MAX_DATE = package$.MODULE$.MAX_DATE();
}
