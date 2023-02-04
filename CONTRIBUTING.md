Contributing to GeoMesa
========================

Project Description
-------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
GeoMesa is an open source suite of tools that enables large-scale geospatial querying and analytics on distributed
computing systems. GeoMesa provides spatio-temporal indexing on top of the Accumulo, HBase and
Cassandra databases for massive storage of point, line, and polygon data. GeoMesa also provides near real time
stream processing of spatio-temporal data by layering spatial semantics on top of Apache Kafka. Through GeoServer,
GeoMesa facilitates integration with a wide range of existing mapping clients over standard OGC (Open Geospatial
Consortium) APIs and protocols such as WFS and WMS. GeoMesa supports Apache Spark for custom distributed
geospatial analytics.
<<<<<<< HEAD
=======
=======
>>>>>>> e771dc363a (typo fixes)
* Help PROJ-users that is less experienced than yourself.
* Write a bug report
* Request a new feature
* Write documentation for your favorite map projection
* Fix a bug
* Implement a new feature
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73e3ca4b45 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 73e3ca4b4 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e771dc363a (typo fixes)
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

<<<<<<< HEAD
<<<<<<< HEAD
- https://www.geomesa.org/
=======
In the following sections you can find some guidelines on how to contribute.
As PROJ is managed on GitHub most contributions require that you have a GitHub
account. Familiarity with [issues](https://guides.github.com/features/issues/) and
the [GitHub Flow](https://guides.github.com/introduction/flow/) is an advantage.
>>>>>>> 6843cbd899f (Merge pull request #3524 from cffk/merid-update-fix)
=======
- http://www.geomesa.org/
>>>>>>> 03e7ed27006 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

Eclipse Contributor Agreement
-----------------------------

Before your contribution can be accepted by the project, you need to create an Eclipse Foundation 
account and electronically sign the Eclipse Contributor Agreement (ECA).

<<<<<<< HEAD
<<<<<<< HEAD
- https://www.eclipse.org/legal/ECA.php 
=======
## Adding bug reports
>>>>>>> 6843cbd899f (Merge pull request #3524 from cffk/merid-update-fix)
=======
- http://www.eclipse.org/legal/ECA.php 
>>>>>>> 03e7ed27006 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

Developer Resources
-------------------

GeoMesa code is hosted on GitHub, and the project is hosted at locationtech:

* https://github.com/locationtech/geomesa
* https://locationtech.org/projects/technology.geomesa

Issue Tracking
--------------

GeoMesa uses JIRA to track ongoing development and issues:

* https://geomesa.atlassian.net/issues/?jql=

Building
--------

See the instructions in the main README.

Contributing
------------

GeoMesa uses git pull requests for contributions. To create a pull request, follow these steps:

<<<<<<< HEAD
<<<<<<< HEAD
* Before starting work, reach out to the community via the [developers list](https://accounts.eclipse.org/mailing-list/geomesa-dev)
  or on [Gitter](https://gitter.im/locationtech/geomesa) to ensure the contribution aligns with the project's goals.
* Most contributions must reference a valid JIRA ticket, unless the change is very small. Sign up for an account at
  https://geomesa.atlassian.net/issues/?jql= and self-assign a ticket. Alternatively, contact the
  [developers list](https://accounts.eclipse.org/mailing-list/geomesa-dev) about doing it for you.
* Ensure that you have signed the Eclipse CLA with the email associated with your GitHub account:
  https://www.eclipse.org/legal/ECA.php
=======
* Contributions to GeoMesa must reference a valid JIRA ticket. Contact the GeoMesa developers list
  about opening a ticket, or to have a ticket assigned to you.
>>>>>>> 02dcbe01abc (GEOMESA-3246 Upgrade Arrow to 11.0.0)
* Fork the GeoMesa project on GitHub - go to https://github.com/locationtech/geomesa and click 'Fork'.
* Create a branch on your forked project that contains your work. See 'Coding Standards', below.
* Use GitHub to open a pull request against the locationtech GeoMesa repository - from your branch on
  GitHub, click 'New Pull Request'.
* Respond to comments on your pull request as they are made.
<<<<<<< HEAD
* When ready, your pull request will be merged by a GeoMesa committer.
=======
If you intend to document one of PROJ's supported projections please use the
[Mercator projection](https://proj.org/operations/projections/merc.html) as a template.
>>>>>>> 8d84cbb0de3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
* When ready, your pull request will be merged by an official GeoMesa contributor.
>>>>>>> 02dcbe01abc (GEOMESA-3246 Upgrade Arrow to 11.0.0)

Coding Standards
----------------

<<<<<<< HEAD
<<<<<<< HEAD
* An initial pull request should be up-to-date with the current main branch.
* The pull request title should consist of a JIRA ticket number followed by a short description, with additional
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 03e7ed27006 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 02dcbe01abc (GEOMESA-3246 Upgrade Arrow to 11.0.0)
* An initial pull request should consist of a single commit, rebased on top of the current main branch.
  * Additional commits can be added in response to code review comments.
* The commit message must consist of a JIRA ticket number followed by a short description, with additional
>>>>>>> 8d84cbb0de3 (Merge pull request #3524 from cffk/merid-update-fix)
  information in bullets below as required.
  * See e.g. https://github.com/locationtech/geomesa/commit/1f345132a717816d5a4951f73b2b73537fce305b
* Code must be reasonably formatted. Scala does not conform well to automatic formatting, but in general
<<<<<<< HEAD
  GeoMesa tries to adhere to the official Scala style guide: https://docs.scala-lang.org/style/
* Code should include unit tests when appropriate.
=======
## Code contributions
>>>>>>> 6843cbd899f (Merge pull request #3524 from cffk/merid-update-fix)
=======
  GeoMesa tries to adhere to the official Scala style guide: http://docs.scala-lang.org/style/
* Code should include unit tests when appropriate.
>>>>>>> 03e7ed27006 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

License and Copyright
---------------------

GeoMesa is provided under the Apache 2 license, and any contributions must maintain this. To ensure proper
licensing, source files must contain an appropriate license header. When a file is created or modified,
the contributor should also indicate their copyright in the header. If copyright is not desired, the contributor
may delegate the copyright to CCRi, as per the default header.

<<<<<<< HEAD
<<<<<<< HEAD
GeoMesa uses the [License Maven Plugin](https://code.mycila.com/license-maven-plugin/) to help manage copyright
headers. This plugin runs as part of the default build, and will fail if any files do not contain a valid header.
To add a new copyright owner, a template file can be placed under `build/copyright/` and added to the
`<validHeaders>` block in the root pom.xml.
=======
Generally speaking the key issues are that those providing code to be included in the repository
understand that the code will be released under the MIT/X license, and that the person providing
the code has the right to contribute the code. For the committer themselves understanding about
the license is hopefully clear. For other contributors, the committer should verify the understanding
unless the committer is very comfortable that the contributor understands the license (for
instance frequent contributors).
>>>>>>> 6843cbd899f (Merge pull request #3524 from cffk/merid-update-fix)
=======
GeoMesa uses the [License Maven Plugin](http://code.mycila.com/license-maven-plugin/) to help manage copyright
headers. This plugin runs as part of the default build, and will fail if any files do not contain a valid header.
To add a new copyright owner, a template file can be placed under `build/copyright/` and added to the
`<validHeaders>` block in the root pom.xml.
>>>>>>> 03e7ed27006 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

Contact
-------

Contact the GeoMesa developers via the developers mailing list:

* https://accounts.eclipse.org/mailing-list/geomesa-dev

For user information, use the users mailing list:

* https://accounts.eclipse.org/mailing-list/geomesa-users
