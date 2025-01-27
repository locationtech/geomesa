package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, TimeUnit}
import scala.util.Random

class IdFunctionFactoryTest extends Specification {

  "IdFunctionFactoryTest" should {
    "generate hashes in a thread-safe way" in {
      testHash("md5")
      testHash("murmurHash3")
      testHash("murmur3_32")
      testHash("murmur3_64")
    }
  }

  def testHash(alg: String): MatchResult[_] = {
    val exp = Expression(s"$alg($$0)")
    val results = Array.fill(3)(Collections.newSetFromMap(new ConcurrentHashMap[AnyRef, java.lang.Boolean]()))
    val exceptions = new CopyOnWriteArrayList[Throwable]()
    val runnables = Array("foo", "bar", "blubaz").map(_.getBytes(StandardCharsets.UTF_8)).zipWithIndex.map { case (input, i) =>
      new Runnable() {
        override def run(): Unit = {
          try { results(i).add(exp.apply(Array(input))) } catch {
            case e: Throwable => exceptions.add(e)
          }
        }
      }
    }
    // baseline results
    runnables.foreach(_.run())

    val r = new Random(-1)
    val pool = new CachedThreadPool(10)
    try {
      var i = 0
      while (i < 1000) {
        pool.submit(runnables(r.nextInt(3)))
        i += 1
      }
    } finally {
      pool.shutdown()
    }
    pool.awaitTermination(1, TimeUnit.SECONDS)
    foreach(results)(_ must haveSize(1))
    exceptions must haveSize(0)
  }
}
