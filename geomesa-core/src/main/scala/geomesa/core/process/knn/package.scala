package geomesa.core.process

package object knn {
  type BoundedNearestNeighbors[T] = BoundedPriorityQueue[T] with NearestNeighbors
}
