#!/bin/bash

EXTNAME="elephant_worker"
EXTSCHEMA="scheduler"


BINDIR=$(dirname "$0")
EXTDIR="${BINDIR}/extension"
if [ ! -d "${EXTDIR}" ]
then
	echo "Not a directory: ${EXTDIR}"
	exit 1
fi

if [ ! -f "${EXTDIR}/version" ]
then
	EXTVERSION="unstable"
else
	EXTVERSION=$(cat "${EXTDIR}/version")
fi

EXTFILE="${EXTDIR}/"${EXTNAME}"--${EXTVERSION}.sql"
TESTFILE="${EXTDIR}/${EXTNAME}--${EXTVERSION}-testsuite.sql"
CONTROLFILE="${EXTDIR}/${EXTNAME}.control"

## Controlfile
cat > "${CONTROLFILE}" <<__EOF__
relocatable = false
superuser   = true
comment 	= 'Job Scheduler using background workers'
default_version = unstable
__EOF__

## Testvariables
PREFILE="${BINDIR}/tests/00_environment.sql"
cat > "${PREFILE}" <<__EOT__
\set extname ${EXTNAME}
\set extschema ${EXTSCHEMA}
__EOT__

cat "${BINDIR}/tests/environment" >> "${PREFILE}"


find "${BINDIR}/scripts" -type f -name '*.sql' | sort | xargs cat > "${EXTFILE}"

find "${BINDIR}/tests" -type f -name '*.sql' | sort | xargs cat > "${TESTFILE}"
