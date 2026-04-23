#!/bin/bash
set -e

if /opt/hive/bin/schematool -dbType postgres -info >/tmp/hive-schema-info.log 2>&1; then
    echo "Hive metastore schema already initialized"
else
    /opt/hive/bin/schematool -dbType postgres -initSchema
fi

exec /opt/hive/bin/hive --service metastore
