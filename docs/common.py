# -*- coding: utf-8 -*-
#
# GeoMesa documentation build configuration file, created by
# sphinx-quickstart on Mon Dec  7 10:39:14 2015.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

import sys
import os
import shlex

# themes/plugins
import sphinx_rtd_theme
from recommonmark.parser import CommonMarkParser

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# Warning: current version numbers are handled in versions.py, which is preprocessed
# by Maven. Do not hardcode current GeoMesa version numbers here!
from target.versions import release,version
import target

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#sys.path.insert(0, os.path.abspath('.'))

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.mathjax',
    'sphinx_tabs.tabs',
    'sphinx_copybutton'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# Custom source parsers
source_parsers = {
    '.md': CommonMarkParser,
}

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'GeoMesa'
# note: shown in our custom footer
<<<<<<< HEAD
copyright = '2013-%(copyright_year)s' % {"copyright_year": target.versions.copyright_year}
author = ''
=======
copyright = u'2013-2021'
author = u''

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# Warning: current version numbers are handled in versions.py, which is preprocessed
# by Maven. Do not hardcode current GeoMesa version numbers here!
from target.versions import release,version,release_version,scala_binary_version
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

# Other versions and variables unlikely to change on every point release
url_github_archive = "https://github.com/locationtech/geomesa/archive"

# RST appended to every file. Used for global substitions.
# (the "%(release)s" substitutions are done by the Python format() method
# prior to appending the RST epilog to each file)
rst_epilog = """

.. _GeoTools: https://geotools.org/

.. _GeoServer: https://geoserver.org/

.. |maven_version| replace:: 3.6.3 or later

<<<<<<< HEAD
.. |geoserver_version| replace:: %(geoserver_version)s

.. |geotools_version| replace:: %(gt_version)s

.. |accumulo_supported_versions| replace:: versions 2.0.1 and %(accumulo_version_recommended)s

.. |accumulo_required_version| replace:: 2.0.1 or %(accumulo_version_recommended)s

.. |hbase_required_version| replace:: %(hbase_version)s

<<<<<<< HEAD
.. |hbase_supported_versions| replace:: versions %(hbase_version)s
=======
.. |hbase_supported_versions| replace:: versions %(hbase_version)s and %(hbase_1_version)s
=======
.. |release_tarball_accumulo| replace:: %(url_locationtech_release)s/geomesa-accumulo-dist_%(scala_binary_version)s/%(release_version)s/geomesa-accumulo_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_kafka| replace:: %(url_locationtech_release)s/geomesa-kafka-dist_%(scala_binary_version)s/%(release_version)s/geomesa-kafka_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_hbase| replace:: %(url_locationtech_release)s/geomesa-hbase-dist_%(scala_binary_version)s/%(release_version)s/geomesa-hbase_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_cassandra| replace:: %(url_locationtech_release)s/geomesa-cassandra-dist_%(scala_binary_version)s/%(release_version)s/geomesa-cassandra_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_source_tarball| replace:: %(url_github_archive)s/geomesa_%(scala_binary_version)s-%(release_version)s.tar.gz

<<<<<<< HEAD
=======
.. |release_tarball_accumulo| replace:: %(url_locationtech_release)s/geomesa-accumulo-dist_%(scala_binary_version)s/%(release_version)s/geomesa-accumulo_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_kafka| replace:: %(url_locationtech_release)s/geomesa-kafka-dist_%(scala_binary_version)s/%(release_version)s/geomesa-kafka_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_hbase| replace:: %(url_locationtech_release)s/geomesa-hbase-dist_%(scala_binary_version)s/%(release_version)s/geomesa-hbase_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_tarball_cassandra| replace:: %(url_locationtech_release)s/geomesa-cassandra-dist_%(scala_binary_version)s/%(release_version)s/geomesa-cassandra_%(scala_binary_version)s-%(release_version)s-bin.tar.gz

.. |release_source_tarball| replace:: %(url_github_archive)s/geomesa_%(scala_binary_version)s-%(release_version)s.tar.gz

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. |maven_version| replace:: 3.5.2 or later

.. |geoserver_version| replace:: 2.17.3

.. |geotools_version| replace:: 23.x

.. |accumulo_required_version| replace:: 1.7.x, 1.8.x, 1.9.x or 2.0.x

.. |accumulo_supported_versions| replace:: versions 1.7.x, 1.8.x, 1.9.x and 2.0.x

.. |hbase_required_version| replace::  1.4.x or 2.2.x

.. |hbase_supported_versions| replace:: versions 1.4.x and 2.2.x

.. |hbase_bundled_version| replace:: 2.2.3
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 759250a5978 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. |hadoop_version| replace:: 2.8 or later

.. |kafka_supported_versions| replace:: versions 2.0 and later

.. |kafka_required_version| replace:: 2.0 or later

.. |cassandra_version| replace:: 3

.. |redis_supported_versions| replace:: versions 5.0 and later

.. |spark_required_version| replace:: %(spark_version)s

.. |spark_supported_versions| replace:: version %(spark_version)s

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. |release_version| replace:: %(release_version)s

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. |release_version_literal| replace:: ``%(release_version)s``
=======
.. |release_version| replace:: ``%(release_version)s``
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

<<<<<<< HEAD
<<<<<<< HEAD
.. |scala_binary_version| replace:: %(scala_binary_version)s

.. |scala_release_version| replace:: ``%(scala_binary_version)s-%(release_version)s``

.. |java_supported_versions| replace:: 11 and 17

""" % {"release": target.versions.release,
       "release_version": target.versions.release_version,
       "scala_binary_version": target.versions.scala_binary_version,
       "geoserver_version": target.versions.geoserver_version,
       "gt_version": target.versions.gt_version,
       "accumulo_version": target.versions.accumulo_version,
       "accumulo_version_recommended": target.versions.accumulo_version_recommended,
       "hbase_version": target.versions.hbase_version,
       "kafka_version": target.versions.kafka_version,
       "spark_version": target.versions.spark_version,
=======
.. |release_version| replace:: ``%(release_version)s``
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
.. |release_version| replace:: %(release_version)s

.. |release_version_literal| replace:: ``%(release_version)s``
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
.. |release_version| replace:: %(release_version)s

.. |release_version_literal| replace:: ``%(release_version)s``
<<<<<<< HEAD
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
.. |release_version| replace:: ``%(release_version)s``
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. |scala_binary_version| replace:: %(scala_binary_version)s

.. |scala_release_version| replace:: ``%(scala_binary_version)s-%(release_version)s``

""" % {"release": release,
       "release_version": release_version,
       "scala_binary_version": scala_binary_version,
<<<<<<< HEAD
       "url_locationtech_release": url_locationtech_release,
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
       "url_locationtech_release": url_locationtech_release,
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
       "url_github_archive": url_github_archive}

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
#today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build']

# The reST default role (used for this markup: `text`) to use for all
# documents.
#default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
#modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
#keep_warnings = False

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
  'canonical_url': 'https://www.geomesa.org/documentation/',
  'collapse_navigation': True,
  'navigation_depth': 4
}

# Add any paths that contain custom themes here, relative to this directory.
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Customized CSS file
html_style = 'css/theme_custom.css'

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
html_short_title = 'GeoMesa'

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#html_logo = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
#html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
#html_domain_indices = True

# If false, no index is generated.
#html_use_index = True

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
# note: shown in our custom footer
html_show_sourcelink = False

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
# note: shown in our custom footer
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
# note: shown in our custom footer
html_show_copyright = False

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Language to be used for generating the HTML full-text search index.
# Sphinx supports the following languages:
#   'da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'ja'
#   'nl', 'no', 'pt', 'ro', 'ru', 'sv', 'tr'
#html_search_language = 'en'

# A dictionary with options for the search language support, empty by default.
# Now only 'ja' uses this config value
#html_search_options = {'type': 'default'}

# The name of a javascript file (relative to the configuration directory) that
# implements a search results scorer. If empty, the default will be used.
#html_search_scorer = 'scorer.js'

# Output file base name for HTML help builder.
htmlhelp_basename = 'GeoMesadoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
# The paper size ('letterpaper' or 'a4paper').
#'papersize': 'letterpaper',

# The font size ('10pt', '11pt' or '12pt').
#'pointsize': '10pt',

# Additional stuff for the LaTeX preamble.
#'preamble': '',

# Latex figure (float) alignment
#'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  (master_doc, 'GeoMesa.tex', u'GeoMesa Documentation',
   u'GeoMesa', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
latex_use_parts = True

# If true, show page references after internal links.
#latex_show_pagerefs = False

# If true, show URL addresses after external links.
#latex_show_urls = False

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'geomesa', u'GeoMesa Documentation',
     [author], 1)
]

# If true, show URL addresses after external links.
#man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  (master_doc, 'GeoMesa', u'GeoMesa Documentation',
   author, 'GeoMesa', 'One line description of project.',
   'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#texinfo_appendices = []

# If false, no module index is generated.
#texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#texinfo_no_detailmenu = False

# strip out the leading `$` in bash and `>` accumulo examples
copybutton_prompt_text = "[$>] "
copybutton_prompt_is_regexp = True
# but keep all lines, as we also have a lot of scala/java examples that don't have prompts
copybutton_only_copy_prompt_lines = False
