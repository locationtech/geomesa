package geomesa.core.filter

import geomesa.core.filter.FilterGenerator._
import geomesa.core.filter.FilterUtils._
import geomesa.core.index.IndexSchema
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.Filter
import org.scalacheck.Gen
import org.scalacheck.Gen._
import scala.collection.JavaConversions._

// These ScalaCheck generators are used to generate arbitrary Filters to test
//  general filter processing.  Not all of them will produce quick queries.
object FilterGenerator {
  def genGeom = oneOf("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))")

  def genTopoPred = Gen.const("INTERSECTS")

  def genTopoString = for {
    geom <- genGeom
    pred <- genTopoPred
  } yield s"$pred$geom"

  def genTopo: Gen[Filter] = genTopoString.map(ECQL.toFilter)

  def dt(int: Interval): String =
    s"(geomesa_index_start_time between '${int.getStart}' AND '${int.getEnd}')"

  def genTimeString =
    oneOf(IndexSchema.everywhen,
      new Interval(new DateTime("2010-07-01T00:00:00.000Z"), new DateTime("2010-07-31T00:00:00.000Z")))

  def genTime = genTimeString.map(i => ECQL.toFilter(dt(i)))

  def genAttr = Gen.choose(0, 100).map(intToAttributeFilter)

  def genAtom = oneOf(genTopo, genTime, genAttr)

  def genNot  = genAtom.map(ff.not)

  def numChildren: Gen[Int] = Gen.frequency(
    (5, 2),
    (3, 3),
    (1, 4)
  )

  def getChildren: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, genFreq)
  } yield c

  def pickBinary: Gen[java.util.List[Filter] => Filter] = oneOf(Seq[java.util.List[Filter] => Filter](ff.or, ff.and))

  val genBinary: Gen[Filter] = for {
    l <- getChildren
    b <- pickBinary
  } yield { b(l) }


  val genBaseFilter: Gen[Filter] = Gen.frequency(
    (2, genTopo),
    (2, genTime),
    (1, genAttr),
    (1, genNot)
  )

  val genFreq: Gen[Filter] = Gen.frequency(
    (2, genBinary),
    (3, genBaseFilter)
  )

  def getChildren2: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, oneOf(genTopo, genAttr))
  } yield c

  // genTime can return filters for Intervals which have 'AND's.
  def genOneLevelBinary[T](f: java.util.List[Filter] => T): Gen[T] =
    getChildren2.map(l => f(l))

  def genOneLevelOr = genOneLevelBinary(ff.or)
  def genOneLevelAnd = genOneLevelBinary(ff.and)
  def genOneLevelAndOr = oneOf(genOneLevelAnd, genOneLevelOr)

  def runSamples[T](gen: Gen[T])(thunk: T => Any) = {
    (0 until 10).foreach { _ => gen.sample.map(thunk) }
  }
}

// These ScalaCheck Generators are used to generate 'small-ish' queries worth testing.
object SmallFilters {

  def getChildrenPositive: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, genBaseFilterPositive)
  } yield c


  val genTreeHeight1: Gen[Filter] = for {
    l <- getChildrenPositive
    b <- pickBinary
  } yield { b(l) }

  val genBaseFilterPositive: Gen[Filter] = Gen.frequency(
    (2, genTopo),
    (2, genTime),
    (1, genAttr)
  )

  val genTreeHeight2: Gen[Filter] = for {
    n <- numChildren
    l <- Gen.listOfN(n, genTreeHeight1)
    b <- pickBinary
  } yield b(l)

  val genSmallTrees: Gen[Filter] = Gen.frequency(
    (1, genTreeHeight1),
    (2, genTreeHeight2),
    (3, genBaseFilterPositive)
  )

  val oneGeomTrees = genSmallTrees.filter(f =>
    decomposeBinary(f).count(_.isInstanceOf[org.opengis.filter.spatial.BinarySpatialOperator]) < 2
  )
}

