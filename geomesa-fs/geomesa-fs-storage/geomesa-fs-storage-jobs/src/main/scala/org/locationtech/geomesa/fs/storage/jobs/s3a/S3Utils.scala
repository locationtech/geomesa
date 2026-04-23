package org.locationtech.geomesa.fs.storage.jobs.s3a

import java.net.URI

object S3Utils {

  def hadoopCompatbleUri(path: URI): URI = {
    if (path.getScheme != "s3") { path } else {
      new URI("s3a", path.getHost, path.getPath, path.getFragment)
    }
  }
}
