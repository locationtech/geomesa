package geomesa.core.process.rank

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 10:43 AM
 */
object MathUtil {
  def squaredDifference(v1: Double, v2: Double) = math.pow(v1 - v2, 2.0)

  def avg(list: List[Double]) = list.reduceLeft(_ + _) / list.length

  def stdDev(list: List[Double], average: Double): Double = list.isEmpty match {
    case false =>
      val squared = list.foldLeft(0.0)(_ + squaredDifference(_, average))
      math.sqrt(squared / list.length.toDouble)
    case true => 0.0
  }

  def nthTriangularNumber(n: Int) = {
    (0 to (n - 1)).sum
  }
}
