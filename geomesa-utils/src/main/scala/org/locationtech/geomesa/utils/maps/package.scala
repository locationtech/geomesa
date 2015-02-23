package org.locationtech.geomesa.utils

package object maps {

  def aggregateMaps[K,V](maps: TraversableOnce[Map[K,V]], operation: (V,V) => V, emptyVal: V): Map[K,V] =
    maps.flatMap(_.toSeq).foldLeft(Map[K,V]()){ case (mapSoFar, (k, v)) =>
      mapSoFar.updated(k, operation(mapSoFar.getOrElse(k, emptyVal), v))
    }

  def sumIntValueMaps[K](maps: TraversableOnce[Map[K,Int]]): Map[K, Int] =
    aggregateMaps(maps, (v1: Int, v2: Int) => v1 + v2, 0)
}
