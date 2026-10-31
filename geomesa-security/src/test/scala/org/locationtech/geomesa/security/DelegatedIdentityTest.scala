/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.specs2.mutable.SpecificationWithJUnit

class DelegatedIdentityTest extends SpecificationWithJUnit {

  "DelegatedIdentity" should {
    "copy and describe the complete delegated identity" in {
      val groups = new java.util.HashSet[String]()
      val auths = new java.util.HashSet[String]()
      groups.add("analysts")
      auths.add("user")
      val identity = new DelegatedIdentity("alice", groups, auths)
      groups.clear()
      auths.clear()
      identity.user() mustEqual "alice"
      identity.groups() mustEqual java.util.Collections.singleton("analysts")
      identity.auths() mustEqual java.util.Collections.singleton("user")
      identity.toString mustEqual "DelegatedIdentity[user=alice, groups=[analysts], auths=[user]]"
      identity.groups().clear() must throwA[UnsupportedOperationException]
      identity.auths().clear() must throwA[UnsupportedOperationException]
      new DelegatedIdentity("", java.util.Collections.emptySet[String](), java.util.Collections.emptySet[String]()) must throwA[IllegalArgumentException]
    }

    "preserve grants independently of transport restrictions" in {
      val grants = new java.util.HashSet[String](java.util.Arrays.asList("", "one,two", "one;two", " team ", "équipe", "a\nb"))
      val identity = new DelegatedIdentity("alice", grants, grants)
      identity.groups() mustEqual grants
      identity.auths() mustEqual grants
    }
  }
}
