#!/bin/bash

function test_and_exit() {
    msg=$1
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
            echo "$msg"
            exit $exitcode
    fi
}

cargo build --release; test_and_exit "stack build failed"
cargo rpm build; test_and_exit "rpm creation failed"
