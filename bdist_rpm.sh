#!/bin/bash

VERSION=$(grep version Cargo.toml | cut -d'"' -f2)
TARBALL_DIR=sarchive-${VERSION}

mkdir -p ~/rpmbuild/SOURCES
mkdir -p ~/rpmbuild/RPMS
mkdir -p ~/rpmbuild/SPECS
mkdir -p ~/rpmbuild/SRPMS

function test_and_exit() {
    msg=$1
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
            echo "$msg"
            exit $exitcode
    fi
}

rm -rf "${TARBALL_DIR}"

cargo build --release; test_and_exit "stack build failed"

mkdir -p "${TARBALL_DIR}"/usr/bin;
mkdir -p "${TARBALL_DIR}"/etc/systemd/system/

cp target/release/sarchive "${TARBALL_DIR}"/usr/bin/
cp data/sarchive.service "${TARBALL_DIR}"/etc/systemd/system/

tar zcvf sarchive-"${VERSION}".tar.gz "${TARBALL_DIR}"; test_and_exit "creating tarball failed"
cp sarchive-"${VERSION}".tar.gz ~/rpmbuild/SOURCES; test_and_exit "copying tarball to SOURCES failed"
cp .rpm/sarchive.spec ~/rpmbuild/SPECS

ls -l ~/rpmbuild/SPECS
ls -l ~/rpmbuild/SOURCES

rpmbuild -ba ~/rpmbuild/SPECS/sarchive.spec; test_and_exit "rpmbuild failed"
