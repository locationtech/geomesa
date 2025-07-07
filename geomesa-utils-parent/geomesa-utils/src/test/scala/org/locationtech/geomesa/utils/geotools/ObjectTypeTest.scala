/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.feature.AttributeTypeBuilder
import org.specs2.mutable.SpecificationWithJUnit

class ObjectTypeTest extends SpecificationWithJUnit {

  "ObjectType" should {
    "throw useful error messages if user data is missing" in {
      ObjectType.selectType(new AttributeTypeBuilder().binding(classOf[java.util.List[_]]).buildDescriptor("foo")) must
        throwAn[IllegalArgumentException](message = "Missing user data key 'subtype' for collection-type field 'foo'")
      ObjectType.selectType(classOf[java.util.List[_]]) must
        throwAn[IllegalArgumentException](message = "Missing user data key 'subtype' for collection-type field")
    }
    "throw useful error messages if user data is wrong type" in {
      ObjectType.selectType(new AttributeTypeBuilder().binding(classOf[java.util.List[_]]).userData("subtype", true).buildDescriptor("foo")) must
        throwAn[IllegalArgumentException](message = "Unexpected user data 'subtype' for collection-type field 'foo': true")
    }
  }
}
