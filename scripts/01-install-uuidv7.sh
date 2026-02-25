#!/bin/sh
set -e

echo "Installing pg_uuidv7 extension..."

# Install build dependencies
apk add --no-cache --virtual .build-deps \
    git \
    make \
    gcc \
    musl-dev \
    postgresql-dev

# Clone and build pg_uuidv7
cd /tmp
git clone https://github.com/fboulnois/pg_uuidv7.git
cd pg_uuidv7
make
make install

# Cleanup
cd /
rm -rf /tmp/pg_uuidv7
apk del .build-deps

echo "pg_uuidv7 extension installed successfully!"
