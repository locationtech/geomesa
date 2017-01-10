GeoMesa Utils
=============

Useful utilities are found in the ``geomesa-utils`` module.

Simple Feature Wrapper Generation
---------------------------------

Tools can generate wrapper classes for simple feature types defined in
TypeSafe Config files. Your config files should be under
src/main/resources. Add the following snippet to your pom, specifying
the package you would like the generated class to reside in:

.. code-block:: xml

    <build>
        ...
        <plugins>
            ...
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>1.3.2</version>
                <executions>
                    <execution>
                        <id>generate-sft-wrappers</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>java</goal>
                        </goals>
                        <configuration>
                            <mainClass>org.locationtech.geomesa.utils.geotools.GenerateFeatureWrappers</mainClass>
                            <cleanupDaemonThreads>false</cleanupDaemonThreads>
                            <killAfter>-1</killAfter>
                            <arguments>
                                <argument>${project.basedir}</argument>
                                <argument>org.foo.mypackage</argument>
                            </arguments>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

.. _geohash:

Geohash
-------

`Geohashes`_ are a geocoding system that uses a `Z-order curve`_ to hierarchically subdivide
the latitude/longitude grid into progressively smaller bins. The length of a Geohash in
bits indicates its precision.

For example, the table below shows Geohash bounding boxes around the point (-78.48, 38.03)
with increasing levels of precision (in units of bits):

==== =============================================================== ======================================= ==========================
prec bounding box                                                    centroid                                Geohash (as binary string)
==== =============================================================== ======================================= ==========================
   1 BBOX(-180.0 -90.0, 0.0, 90.0)                                   POINT (-90 0)                           0
   2 BBOX(-180.0 0.0, 0.0, 90.0)                                     POINT (-90 45)                          01
   3 BBOX(-90.0 0.0, 0.0, 90.0)                                      POINT (-45 45)                          011
   4 BBOX(-90.0 0.0, 0.0, 45.0)                                      POINT (-45 22.5)                        0110
   5 BBOX(-90.0 0.0, -45.0, 45.0)                                    POINT (-67.5 22.5)                      01100
   6 BBOX(-90.0 22.5, -45.0, 45.0)                                   POINT (-67.5 33.75)                     011001
   7 BBOX(-90.0 22.5, -67.5, 45.0)                                   POINT (-78.75 33.75)                    0110010
   8 BBOX(-90.0 33.75, -67.5, 45.0)                                  POINT (-78.75 39.375)                   01100101
   9 BBOX(-78.75 33.75, -67.5, 45.0)                                 POINT (-73.125 39.375)                  011001011
  10 BBOX(-78.75 33.75, -67.5, 39.375)                               POINT (-73.125 36.5625)                 0110010110
  11 BBOX(-78.75 33.75, -73.125, 39.375)                             POINT (-75.9375 36.5625)                01100101100
  12 BBOX(-78.75 36.5625, -73.125, 39.375)                           POINT (-75.9375 37.96875)               011001011001
  13 BBOX(-78.75 36.5625, -75.9375, 39.375)                          POINT (-77.34375 37.96875)              0110010110010
  14 BBOX(-78.75 37.96875, -75.9375, 39.375)                         POINT (-77.34375 38.671875)             01100101100101
  15 BBOX(-78.75 37.96875, -77.34375, 39.375)                        POINT (-78.046875 38.671875)            011001011001010
  16 BBOX(-78.75 37.96875, -77.34375, 38.671875)                     POINT (-78.046875 38.3203125)           0110010110010100
  17 BBOX(-78.75 37.96875, -78.046875, 38.671875)                    POINT (-78.3984375 38.3203125)          01100101100101000
  18 BBOX(-78.75 37.96875, -78.046875, 38.3203125)                   POINT (-78.3984375 38.14453125)         011001011001010000
  19 BBOX(-78.75 37.96875, -78.3984375, 38.3203125)                  POINT (-78.57421875 38.14453125)        0110010110010100000
  20 BBOX(-78.75 37.96875, -78.3984375, 38.14453125)                 POINT (-78.57421875 38.056640625)       01100101100101000000
  21 BBOX(-78.57421875 37.96875, -78.3984375, 38.14453125)           POINT (-78.486328125 38.056640625)      011001011001010000001
  22 BBOX(-78.57421875 37.96875, -78.3984375, 38.056640625)          POINT (-78.486328125 38.0126953125)     0110010110010100000010
  23 BBOX(-78.486328125 37.96875, -78.3984375, 38.056640625)         POINT (-78.4423828125 38.0126953125)    01100101100101000000101
  24 BBOX(-78.486328125 38.0126953125, -78.3984375, 38.056640625)    POINT (-78.4423828125 38.03466796875)   011001011001010000001011
  25 BBOX(-78.486328125 38.0126953125, -78.4423828125, 38.056640625) POINT (-78.46435546875 38.03466796875)  0110010110010100000010110
==== =============================================================== ======================================= ==========================

The ``org.locationtech.geomesa.utils.geohash.GeoHash`` class provides tools for working
with Geohashes. The table above may be generated with the following Scala code:

.. code-block:: scala

    import org.locationtech.geomesa.utils.geohash.GeoHash

    val lon = -78.48
    val lat =  38.03

    for (p <- 1 to 25) {
      val gh = GeoHash(lon, lat, p)
      val bbox = gh.bbox
      val bboxString = s"BBOX(${bbox.minLon} ${bbox.minLat}, ${bbox.maxLon}, ${bbox.maxLat})"
      println("%4d %-63s %-38s %s".format(p, bboxString, gh.getPoint, gh.toBinaryString))
    }

.. _geohash_base32:

Base-32 Encoding
^^^^^^^^^^^^^^^^

Geohashes are encoded as strings with the following base-32 representation:

=== ====== =======
dec binary base-32
=== ====== =======
0   00000  0
1   00001  1
2   00010  2
3   00011  3
4   00100  4
5   00101  5
6   00110  6
7   00111  7
8   01000  8
9   01001  9
10  01010  b
11  01011  c
12  01100  d
13  01101  e
14  01110  f
15  01111  g
16  10000  h
17  10001  j
18  10010  k
19  10011  m
20  10100  n
21  10101  p
22  10110  q
23  10111  r
24  11000  s
25  11001  t
26  11010  u
27  11011  v
28  11100  w
29  11101  x
30  11110  y
31  11111  z
=== ====== =======

By this convention, the 25-bit Geohash that contains (-78.48, 38.03) described above would be encoded as "dqb0q"::

    01100 10110 01010 00000 10110
    ----- ----- ----- ----- -----
      d     q     b     0     q


.. _Geohashes: https://en.wikipedia.org/wiki/Geohash

.. _Z-order curve: https://en.wikipedia.org/wiki/Z-order_curve