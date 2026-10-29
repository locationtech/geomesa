/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.http.HttpHeaders

import java.util.concurrent.{Callable, Executors, TimeUnit}

@RunWith(classOf[JUnitRunner])
class ThreadLocalAuthorizationsProviderTest extends Specification {

  import ThreadLocalAuthorizationsProvider.{withAuthorizations, wrap}

  private def toHttpHeaders(values: (String, String)*): HttpHeaders = {
    val headers = new HttpHeaders()
    values.foreach { case (name, value) => headers.add(name, value) }
    headers
  }

  "ThreadLocalAuthorizationsProvider" should {
    "copy header values verbatim into a case-insensitive, read-only snapshot" in {
      val source = toHttpHeaders("Authorization" -> " opaque secret ")
      ThreadLocalAuthorizationsProvider.withRequestHeaders(source) {
        val context = ThreadLocalAuthorizationsProvider.requestHeaders
        source.get("Authorization").set(0, "changed")
        context.getFirst("AUTHORIZATION") mustEqual " opaque secret "
        context.set("Authorization", "changed") must throwA[UnsupportedOperationException]
        context.get("Authorization").add("changed") must throwA[UnsupportedOperationException]
      }
    }

    "preserve Spring's multiple-value header representation in the scoped snapshot" in {
      val source = toHttpHeaders("X-Test" -> "first", "x-test" -> "second")
      ThreadLocalAuthorizationsProvider.withRequestHeaders(source) {
        ThreadLocalAuthorizationsProvider.requestHeaders.get("X-Test") mustEqual
          java.util.List.of("first", "second")
      }
    }

    "carry opaque headers across executor hops without leaking into the next request" in {
      val executor = Executors.newSingleThreadExecutor()
      val seen = new java.util.concurrent.CopyOnWriteArrayList[String]()
      try {
        val alice = toHttpHeaders("Authorization" -> "alice-token")
        val bob = toHttpHeaders("Authorization" -> "bob-token")
        val tasks = Seq(alice, bob).map { context =>
          ThreadLocalAuthorizationsProvider.withRequestHeaders(context) {
            withAuthorizations(java.util.Collections.singletonList("U")) {
              wrap(() => { seen.add(ThreadLocalAuthorizationsProvider.requestHeaders.getFirst("authorization")); () })
            }
          }
        }
        alice.set("Authorization", "changed after capture")
        tasks.foreach(task => executor.submit(task).get(10, TimeUnit.SECONDS))
        seen.toArray.toSeq mustEqual Seq("alice-token", "bob-token")
        executor.submit(new Callable[Boolean] {
          override def call(): Boolean = ThreadLocalAuthorizationsProvider.requestHeaders.isEmpty
        }).get(10, TimeUnit.SECONDS) must beTrue
      } finally { executor.shutdownNow() }
    }

    "restore outer header scopes after exceptions" in {
      ThreadLocalAuthorizationsProvider.withRequestHeaders(toHttpHeaders("Authorization" -> "outer")) {
        ThreadLocalAuthorizationsProvider.withRequestHeaders[Unit](toHttpHeaders("Authorization" -> "inner")) {
          throw new IllegalStateException("test")
        } must throwA[IllegalStateException]
        ThreadLocalAuthorizationsProvider.requestHeaders.getFirst("Authorization") mustEqual "outer"
      }
      ThreadLocalAuthorizationsProvider.requestHeaders.isEmpty must beTrue
    }

    "restore nested scopes after exceptions and copy mutable input" in {
      val provider = new ThreadLocalAuthorizationsProvider
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
      val provider = new ThreadLocalAuthorizationsProvider
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

    "preserve authorization ceilings in propagated tasks" in {
      val provider = new FilteringAuthorizationsProvider(
        new ThreadLocalAuthorizationsProvider, java.util.Arrays.asList[String]("A"))
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
