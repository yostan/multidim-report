#! /bin/bash

sed -i 's/NUMBER/DECIMAL/g;s/VARCHAR2/VARCHAR/g;s/[^_\W]DATE/ TIMESTAMP/g;s/NOT NULL//g;s/not null//g;s/create table/create external table if not exists/g' $1

