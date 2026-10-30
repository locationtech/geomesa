/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.{Callable, Executors, TimeUnit}

@RunWith(classOf[JUnitRunner])
class DelegatedAuthorizationsProviderTest extends Specification {

  import DelegatedAuthorizationsProvider.{withAuthorizations, wrap}

  "DelegatedAuthorizationsProvider" should {
    "copy and redact the complete delegated identity" in {
      val groups = new java.util.HashSet[String]()
      val auths = new java.util.HashSet[String]()
      groups.add("analysts")
      auths.add("U")
      val identity = new DelegatedIdentity("alice", groups, auths)
      groups.clear()
      auths.clear()
      identity.user() mustEqual "alice"
      identity.groups() mustEqual java.util.Collections.singleton("analysts")
      identity.auths() mustEqual java.util.Collections.singleton("U")
      identity.toString must not(contain("alice"))
      identity.groups().clear() must throwA[UnsupportedOperationException]
      new DelegatedIdentity("", java.util.Collections.emptySet[String](), java.util.Collections.emptySet[String]()) must throwA[IllegalArgumentException]
      new DelegatedIdentity("alice", java.util.Collections.singleton("one,two"), java.util.Collections.emptySet[String]()) must
        throwA[IllegalArgumentException]
    }

    "distinguish absent identity from a user with empty grants" in {
      val provider = new DelegatedAuthorizationsProvider
      val identity = new DelegatedIdentity("alice", java.util.Collections.emptySet[String](), java.util.Collections.emptySet[String]())
      DelegatedAuthorizationsProvider.withIdentity(identity) {
        DelegatedAuthorizationsProvider.identity.get() mustEqual identity
        provider.getAuthorizations.isEmpty must beTrue
      }
      DelegatedAuthorizationsProvider.identity.isEmpty must beTrue
    }

    "carry complete identities across executor hops without leaking into the next request" in {
      val executor = Executors.newSingleThreadExecutor()
      val seen = new java.util.concurrent.CopyOnWriteArrayList[DelegatedIdentity]()
      val provider = new DelegatedAuthorizationsProvider
      try {
        val alice = new DelegatedIdentity("alice", java.util.Collections.singleton("analysts"), java.util.Collections.singleton("U"))
        val bob = new DelegatedIdentity("bob", java.util.Collections.singleton("operators"), java.util.Collections.singleton("FOUO"))
        val tasks = Seq(alice, bob).map { identity =>
          DelegatedAuthorizationsProvider.withIdentity(identity) {
            wrap(() => {
              seen.add(DelegatedAuthorizationsProvider.identity.get())
              assert(provider.getAuthorizations.containsAll(identity.auths()))
            })
          }
        }
        tasks.foreach(task => executor.submit(task).get(10, TimeUnit.SECONDS))
        seen.toArray.toSeq mustEqual Seq(alice, bob)
        executor.submit(new Callable[Boolean] {
          override def call(): Boolean = DelegatedAuthorizationsProvider.identity.isEmpty && provider.getAuthorizations.isEmpty
        }).get(10, TimeUnit.SECONDS) must beTrue
      } finally { executor.shutdownNow() }
    }

    "restore both identity and authorizations after a nested failure" in {
      val provider = new DelegatedAuthorizationsProvider
      val alice = new DelegatedIdentity("alice", java.util.Collections.singleton("analysts"), java.util.Collections.singleton("U"))
      val bob = new DelegatedIdentity("bob", java.util.Collections.singleton("operators"), java.util.Collections.singleton("FOUO"))
      DelegatedAuthorizationsProvider.withIdentity(alice) {
        DelegatedAuthorizationsProvider.withIdentity[Unit](bob) {
          throw new IllegalStateException("test")
        } must throwA[IllegalStateException]
        DelegatedAuthorizationsProvider.identity.get() mustEqual alice
        provider.getAuthorizations mustEqual java.util.Collections.singletonList("U")
      }
      DelegatedAuthorizationsProvider.identity.isEmpty must beTrue
      provider.getAuthorizations.isEmpty must beTrue
    }

    "restore nested scopes after exceptions and copy mutable input" in {
      val provider = new DelegatedAuthorizationsProvider
      val auths = new java.util.ArrayList[String]()
      auths.add("A")
      withAuthorizations(auths) {
        auths.add("C")
        provider.getAuthorizations mustEqual java.util.Arrays.asList[String]("A")
        withAuthorizations[Unit](java.util.Arrays.asList[String]("B")) {
          throw new IllegalStateException("test")
        } must throwA[IllegalStateException]
        provider.getAuthorizations mustEqual java.util.Arrays.asList[String]("A")
      }
      provider.getAuthorizations.isEmpty must beTrue
    }

    "capture at submission and restore a reused executor thread after success and failure" in {
      val provider = new DelegatedAuthorizationsProvider
      val executor = Executors.newSingleThreadExecutor()
      try {
        // Capture both users before executing either task, so the submitter's scope has ended.
        val seen = new java.util.concurrent.CopyOnWriteArrayList[java.util.List[String]]()
        val first = withAuthorizations(java.util.Arrays.asList[String]("A")) {
          wrap(() => { seen.add(provider.getAuthorizations); () })
        }
        val second = withAuthorizations(java.util.Arrays.asList[String]("B")) {
          wrap(() => { seen.add(provider.getAuthorizations); throw new IllegalStateException("test") })
        }
        executor.submit(first).get(10, TimeUnit.SECONDS)
        executor.submit(second).get(10, TimeUnit.SECONDS) must
          throwA[java.util.concurrent.ExecutionException]
        seen.get(0) mustEqual java.util.Arrays.asList[String]("A")
        seen.get(1) mustEqual java.util.Arrays.asList[String]("B")
        executor.submit(new Callable[java.util.List[String]] {
          override def call(): java.util.List[String] = provider.getAuthorizations
        }).get(10, TimeUnit.SECONDS).isEmpty must beTrue
      } finally {
        executor.shutdownNow()
      }
    }

    "retain narrowed auths when propagating a complete identity" in {
      val provider = new DelegatedAuthorizationsProvider
      val auths = new java.util.HashSet[String](java.util.Arrays.asList("A", "C"))
      val identity = new DelegatedIdentity("alice", java.util.Collections.emptySet[String](), auths)
      val task = DelegatedAuthorizationsProvider.withIdentity(identity) {
        withAuthorizations(java.util.Collections.singletonList("A")) {
          wrap(() => {
            assert(DelegatedAuthorizationsProvider.identity.get() eq identity)
            assert(provider.getAuthorizations == java.util.Collections.singletonList("A"))
          })
        }
      }
      task.run()
      DelegatedAuthorizationsProvider.identity.isEmpty must beTrue
      provider.getAuthorizations.isEmpty must beTrue
    }

    "preserve authorization ceilings in propagated tasks" in {
      val provider = new FilteringAuthorizationsProvider(
        new DelegatedAuthorizationsProvider, java.util.Arrays.asList[String]("A"))
      val executor = Executors.newSingleThreadExecutor()
      try {
        val seen = new java.util.concurrent.atomic.AtomicReference[java.util.List[String]]()
        val task = withAuthorizations(java.util.Arrays.asList[String]("A", "C")) {
          wrap(() => seen.set(provider.getAuthorizations))
        }
        executor.submit(task).get(10, TimeUnit.SECONDS)
        seen.get() mustEqual java.util.Arrays.asList[String]("A")
      } finally {
        executor.shutdownNow()
      }
    }
  }
}
