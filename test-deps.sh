#!/bin/bash

cd $(dirname $0)
set -e

ORLEANS_TAG="v2.3.4"

if ! [ -d "orleans" ]; then
    git clone --single-branch --branch "$ORLEANS_TAG" --depth 1 "https://github.com/dotnet/orleans.git"
fi

pushd orleans/test

TEST_PATH="../../test/Orleans.Redis.FunctionalTests"

rm -rf \
    "${TEST_PATH}/TestInternal"

cp --parents "TesterInternal/MembershipTests/MembershipTableTestsBase.cs" "${TEST_PATH}"
cp --parents "TestInfrastructure/TestExtensions/SiloAddressUtils.cs" "${TEST_PATH}"
