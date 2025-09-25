import sys, os
sys.path.append(os.path.abspath('.'))
from common import *

# Warning: current version numbers are handled in versions.py, which is preprocessed
# by Maven. Do not hardcode current GeoMesa version numbers here!
import target

# Suffix of source filenames
source_suffix = '.rst'

# Encoding of source files
source_encoding = 'utf-8'

# Master toctree document
master_doc = 'index'

# HTML title
html_title = 'GeoMesa %s Manuals' % release

exclude_patterns = [ 'README.md' ]

# replacement in code blocks - https://github.com/sphinx-doc/sphinx/issues/4054#issuecomment-329097229
def ultimateReplace(app, docname, source):
  result = source[0]
  for key in app.config.ultimate_replacements:
    result = result.replace(key, app.config.ultimate_replacements[key])
  source[0] = result


ultimate_replacements = {
  "{{accumulo_supported_versions}}": "versions 2.0.1 and " + target.versions.accumulo_version,
  "{{accumulo_required_version}}": "2.0.1 or " + target.versions.accumulo_version,
  "{{cassandra_supported_versions}}": "version " + target.versions.cassandra_version,
  "{{cassandra_required_version}}": target.versions.cassandra_version,
  "{{geoserver_version}}": target.versions.geoserver_version,
  "{{hbase_supported_versions}}": "version " + target.versions.hbase_version,
  "{{hbase_required_version}}": target.versions.hbase_version,
  "{{hadoop_supported_versions}}": "versions " + target.versions.hadoop_min_version + " and later",
  "{{hadoop_required_version}}": target.versions.hadoop_min_version + " or later",
  "{{java_supported_versions}}": "versions 17 and 21",
  "{{java_required_version}}": "17 or 21",
  "{{kafka_supported_versions}}": "versions " + target.versions.kafka_min_version + " and later",
  "{{kafka_required_version}}": target.versions.kafka_min_version + " or later",
  "{{maven_required_version}}": target.versions.maven_min_version + " or later",
  "{{micrometer_version}}": target.versions.micrometer_version,
  "{{postgres_supported_versions}}": "versions " + target.versions.postgres_min_version + " and later",
  "{{prometheus_version}}": target.versions.prometheus_version,
  "{{redis_supported_versions}}": "versions " + target.versions.redis_min_version + " and later",
  "{{redis_required_version}}": target.versions.redis_min_version + " or later",
  "{{release}}": target.versions.release,
  "{{scala_binary_version}}": target.versions.scala_binary_version,
  "{{spark_supported_versions}}": "version " + target.versions.spark_version,
  "{{spark_required_version}}": target.versions.spark_version,
}


def setup(app):
  app.add_css_file('https://fonts.googleapis.com/css?family=Roboto:400,700')
  app.add_config_value('ultimate_replacements', {}, True)
  app.connect('source-read', ultimateReplace)
