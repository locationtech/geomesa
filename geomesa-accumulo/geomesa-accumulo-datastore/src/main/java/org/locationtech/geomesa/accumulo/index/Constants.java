/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index;

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
