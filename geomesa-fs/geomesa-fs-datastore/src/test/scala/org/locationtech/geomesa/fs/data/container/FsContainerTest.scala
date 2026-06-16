package org.locationtech.geomesa.fs.data.container

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.data.container
import org.locationtech.geomesa.fs.data.container.FsContainerTest.IcebergRestContainer
import org.slf4j.LoggerFactory
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{GenericContainer, MinIOContainer, Network}
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

trait FsContainerTest extends BeforeAfterAll with LazyLogging {

  private val network = Network.newNetwork()

  private val minio =
    new MinIOContainer(DockerImageName.parse("minio/minio").withTag(sys.props("minio.docker.tag")))
      .withNetwork(network)
      .withNetworkAliases("minio")

  private val iceberg =
    new IcebergRestContainer()
      .withNetwork(network)
      .withNetworkAliases("rest-catalog")

  private val postgres =
    new PostgreSQLContainer(DockerImageName.parse("postgres").withTag(sys.props("postgres.docker.tag")).asCompatibleSubstituteFor("postgres"))
      .withDatabaseName("postgres") // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
      .withUsername("postgres")

  private lazy val s3Configs =
    s"""fs.s3.region=us-east-1
       |fs.s3.endpoint=${minio.getS3URL}
       |fs.s3.access-key-id=${minio.getUserName}
       |fs.s3.secret-access-key=${minio.getPassword}
       |fs.s3.force-path-style=true""".stripMargin

  protected lazy val fileParams = Map(
    "fs.path" -> s"s3://geomesa/fs/file/",
    "geomesa.security.auths" -> "user",
    "fs.config.properties" ->
      s"""fs.metadata.type=file
         |$s3Configs
         |""".stripMargin
  )

  protected lazy val jdbcParams = {
    postgres.start()
    Map(
      "fs.path" -> s"s3://geomesa/fs/jdbc/",
      "geomesa.security.auths" -> "user",
      "fs.config.properties" ->
        s"""fs.metadata.type=jdbc
           |fs.metadata.jdbc.url=${postgres.getJdbcUrl}
           |fs.metadata.jdbc.user=${postgres.getUsername}
           |fs.metadata.jdbc.password=${postgres.getPassword}
           |$s3Configs
           |""".stripMargin
    )
  }

  protected lazy val icebergParams = {
    iceberg.start()
    Map(
      "fs.path" -> s"s3://geomesa/fs/iceberg/",
      "geomesa.security.auths" -> "user",
      "fs.config.properties" ->
        s"""fs.metadata.type=iceberg
           |iceberg.uri=http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/"
           |# note: s3 analytics/crt throws dns errors with the minio endpoint, either due to localhost or the use of a port
           |#iceberg.s3.analytics-accelerator.enabled=true
           |#iceberg.s3.crt.enabled=true
           |$s3Configs
           |""".stripMargin
    )
  }

  override def beforeAll(): Unit = {
    minio.start()
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/geomesa")
    if (logger.underlying.isDebugEnabled()) {
      postgres.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgres")))
      postgres.setCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all")
    }
  }

  override def afterAll(): Unit = {
    postgres.stop()
    iceberg.stop()
    minio.stop()
    network.close()
  }
}

object FsContainerTest {

  class IcebergRestContainer
      extends GenericContainer[IcebergRestContainer](DockerImageName.parse("tabulario/iceberg-rest").withTag(sys.props("iceberg.rest.docker.tag"))) {
    withExposedPorts(8181)
    // Override the upstream image's malformed default URI (jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory)
    // `mode=memory` ended up in the filename instead of as a query parameter. Also add a busy_timeout, so transient
    // contention from Iceberg's connection pool doesn't show up as SQLITE_BUSY 500s during multi-table ingests
    withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL&synchronous=NORMAL&busy_timeout=30000")
    withEnv("CATALOG_WAREHOUSE", "s3://warehouse/iceberg")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
  }
}
