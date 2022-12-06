/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

/**
 * AvroSimpleFeature version 2 changes serialization of Geometry types from
 * WKT (Well Known Text) to WKB (Well Known Binary)
 */
@deprecated("Deprecated with no replacement")
object Version2Deserializer extends ASFDeserializer
