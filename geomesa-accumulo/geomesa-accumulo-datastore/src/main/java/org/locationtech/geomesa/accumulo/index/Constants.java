/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index;

import org.joda.time.DateTime;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;

/**
 * This is a utility class to allow for simpler use of Scala constants within
 * the Java portion of the API.
 */
public class Constants {
  public final static String SF_PROPERTY_START_TIME = SimpleFeatureTypes.Configs$.MODULE$.DEFAULT_DATE_KEY();
  public final static String SFT_INDEX_SCHEMA       = SimpleFeatureTypes.Configs$.MODULE$.ST_INDEX_SCHEMA_KEY();

  @Deprecated
  public final static String SF_PROPERTY_GEOMETRY   = "geomesa_index_geometry";
  @Deprecated
  public final static String SF_PROPERTY_END_TIME   = "geomesa_index_end_time";

  public final static DateTime MIN_DATE = package$.MODULE$.MIN_DATE();
  public final static DateTime MAX_DATE = package$.MODULE$.MAX_DATE();
}
