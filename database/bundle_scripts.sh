#!/bin/bash


BINDIR=$(dirname "$0")
EXTDIR="${BINDIR}/extension"
if [ ! -d "${EXTDIR}" ]
then
	echo "Not a directory: ${EXTDIR}"
	exit 1
fi

if [ -f "${EXTDIR}/version" ]
then
	EXTFILE="${EXTDIR}/elephant_worker--unstable.sql"
	TESTFILE="${EXTDIR}/elephant-worker--unstable-testsuite.sql"
else
	EXTVERSION=$(cat "${EXTDIR}/version")
	EXTFILE="${EXTDIR}/elephant_worker--${EXTVERSION}.sql"
	TESTFILE="${EXTDIR}/elephant-worker--${EXTVERSION}-testsuite.sql"
fi

find "${BINDIR}/scripts" -type f -name '*.sql' | sort | xargs cat > "${EXTFILE}"

find "${BINDIR}/tests" -type f -name '*.sql' | sort | xargs cat > "${TESTFILE}"
