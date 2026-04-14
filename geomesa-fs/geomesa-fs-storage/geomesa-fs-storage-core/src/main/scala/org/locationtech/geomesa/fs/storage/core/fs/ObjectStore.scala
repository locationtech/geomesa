package org.locationtech.geomesa.fs.storage.core.fs

import java.net.URI

trait ObjectStore {

  /**
   * Get the size of the object stored at this path
   *
   * @param path file path
   * @return
   */
  def size(path: URI): Long

  /**
   * Copy a file
   *
   * @param from from
   * @param to to
   */
  def copy(from: URI, to: URI): Unit

  /**
   * Delete a file
   *
   * @param path file path
   */
  def delete(path: URI): Unit
}

object ObjectStore {

}
