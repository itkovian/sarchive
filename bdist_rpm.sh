#!/bin/bash

function test_and_exit() {
    msg=$1
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
            echo "$msg"
            exit $exitcode
    fi
}

cargo install cargo-generate-rpm

cargo build --release --all-features; test_and_exit "stack build failed"
#strip -s ${CARGO_TARGET_DIR}/release/sarchive; test_and_exit "strip failed"
cargo generate-rpm; test_and_exit "rpm creation failed"
