Contributing to GeoMesa
========================

Project Description
-------------------

GeoMesa is an open source suite of tools that enables large-scale geospatial querying and analytics on distributed
computing systems. GeoMesa provides spatio-temporal indexing on top of the Accumulo, HBase and
Cassandra databases for massive storage of point, line, and polygon data. GeoMesa also provides near real time
stream processing of spatio-temporal data by layering spatial semantics on top of Apache Kafka. Through GeoServer,
GeoMesa facilitates integration with a wide range of existing mapping clients over standard OGC (Open Geospatial
Consortium) APIs and protocols such as WFS and WMS. GeoMesa supports Apache Spark for custom distributed
geospatial analytics.

- https://www.geomesa.org/

Eclipse Contributor Agreement
-----------------------------

Before your contribution can be accepted by the project, you need to create an Eclipse Foundation 
account and electronically sign the Eclipse Contributor Agreement (ECA).

- https://www.eclipse.org/legal/ECA.php 

Developer Resources
-------------------

GeoMesa code is hosted on GitHub, and the project is hosted at locationtech:

* https://github.com/locationtech/geomesa
* https://locationtech.org/projects/technology.geomesa

Issue Tracking
--------------

GeoMesa uses GitHub to track ongoing development and issues:

* https://github.com/locationtech/geomesa/issues

Building
--------

See the instructions in the main README.

Contributing
------------

GeoMesa uses git pull requests for contributions. To create a pull request, follow these steps:

* Before starting work, reach out to the community to ensure the contribution aligns with the project's goals.
  This can be accomplished by creating an [issue](https://github.com/locationtech/geomesa/issues), a
  [discussion](https://github.com/locationtech/geomesa/discussions), emailing the
  [developers list](https://accounts.eclipse.org/mailing-list/geomesa-dev), or posting on
  [Gitter](https://gitter.im/locationtech/geomesa).
* Ensure that you have signed the Eclipse CLA with the email associated with your GitHub account:
  https://www.eclipse.org/legal/ECA.php
* Fork the GeoMesa project on GitHub - go to https://github.com/locationtech/geomesa and click 'Fork'.
* Create a branch on your forked project that contains your work. See 'Coding Standards', below.
* Use GitHub to open a pull request against the locationtech GeoMesa repository - from your branch on
  GitHub, click 'New Pull Request'.
* Respond to comments on your pull request as they are made.
* When ready, your pull request will be merged by a GeoMesa committer.

Coding Standards
----------------

* An initial pull request should be up-to-date with the current main branch.
* The pull request title should reference the relevant component, as applicable (e.g. `Docs - ...`).
* The pull request description should reference any issue numbers, with additional information in bullets below as required.
* Code must be reasonably formatted. Scala does not conform well to automatic formatting, but in general
  GeoMesa tries to adhere to the official Scala style guide: https://docs.scala-lang.org/style/
* Code should include unit tests when appropriate.

License and Copyright
---------------------

GeoMesa is provided under the Apache 2 license, and any contributions must maintain this. To ensure proper
licensing, source files must contain an appropriate license header. When a file is created or modified,
the contributor should also indicate their copyright in the header. If copyright is not desired, the contributor
may delegate the copyright to CCRi, as per the default header.

GeoMesa uses the [License Maven Plugin](https://code.mycila.com/license-maven-plugin/) to help manage copyright
headers. This plugin runs as part of the default build, and will fail if any files do not contain a valid header.
To add a new copyright owner, a template file can be placed under `build/copyright/` and added to the
`<validHeaders>` block in the root pom.xml.

Releasing this Project
----------------------

Project maintainers can cut a release using the script `./build/scripts/do-release.sh`, which will perform the following steps:

* Trigger and monitor GitHub actions to tag and build the release
* Download the release artifacts and sign them with the user's gpg key
* Publish the Maven artifacts to Maven central
* Publish the binary distribution bundles to a GitHub release
* Trigger and monitor a GitHub action to publish the release docs to geomesa.org

The following prerequisites are required for running the script:

* `gh` (the GitHub CLI tool) must be installed and configured with the correct credentials for this repo
* `gpg` must be installed and configured with an appropriate key
* Sonatype credentials for publishing to Maven central must be available in the user's `~/.m2/settings.xml` under `<id>sonatype</id>`

See https://central.sonatype.org/register/central-portal/ for full details on preparing to publish to Maven central.

Contact
-------

* [GitHub Discussions](https://github.com/locationtech/geomesa/discussions)
* [Developer mailing list](https://accounts.eclipse.org/mailing-list/geomesa-dev)
* [User mailing list](https://accounts.eclipse.org/mailing-list/geomesa-users)
* [Community chat on Gitter](https://gitter.im/locationtech/geomesa)
