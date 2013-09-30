<?xml version="1.0" encoding="utf-8"?>
<!--
  ~ Copyright 2013 Commonwealth Computer Research, Inc.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<!-- modified from an original copy at this URL:
  http://code.google.com/p/docbkx-tools/source/browse/trunk/docbkx-samples/src/docbkx/docbook-fo.xsl -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format"
                version="1.0">
    <!-- import the main stylesheet, here pointing to fo/docbook.xsl -->
    <xsl:import href="urn:docbkx:stylesheet"/>
    <!-- highlight.xsl must be imported in order to enable highlighting support, highlightSource=1 parameter
 is not sufficient -->
    <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>

    <xsl:attribute-set name="verbatim.properties">
        <xsl:attribute name="background-color">#EEEEEE</xsl:attribute>
        <xsl:attribute name="padding">0.08in</xsl:attribute>
        <xsl:attribute name="font-size">9pt</xsl:attribute>
    </xsl:attribute-set>

</xsl:stylesheet>