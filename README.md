<!--
title: CockroachDB
weight: 4705
-->
# CockroachDB

**This plugin is in ALPHA stage**

This activity allows you to populate data into CockroachDB.

## Installation

### Flogo CLI
```bash
flogo install github.com/codelity-co/flogo-cockroachdb-activity
```

## Configuration

### Settings:
  | Name                | Type   | Description
  | :---                | :---   | :---
  | host                | string | CockroachDB host string, e.g. localhost:26257 - ***REQUIRED***
  | database            | string | CockroachDB database name - ***REQUIRED***
  | user                | string | CockroachDB user name - ***REQUIRED***
  | password            | string | CockroachDB user password - ***REQUIRED***
  | options             | object | CockroachDB connection options
  | dataMapping         | object | JSON Path Mapping to CockroachDB multiple tables - ***REQUIRED*** 


### Input
  | Name                | Type   | Description
  | :---                | :---   | :---
  | data                | object | data object, data mapping will apply JSON path against this data object - ***REQUIRED***

### Output:
  | Name          | Type   | Description
  | :---          | :---   | :---
  | status        | string | status text, ERROR or SUCCESS - ***REQUIRED***
  | result        | any    | activity result


## Example

```json
{
  "id": "flogo-cockroachdb-activity",
  "name": "Codelity Flogo CockroachDB Activity",
  "ref": "github.com/codelity-co/flogo-cockroachdb-activity",
  "settings": {
    "host": "localhost:26257",
    "database": "testdb",
    "user": "testuser",
    "password": "testuser",
    "dataMapping": {
      "tableA": {
        "field1": "$.attr1",
        "field2": "$.attr2"
      },
      "tableB": {
        "field1": "$.attr1",
        "field3": "$.attr3"
      }
    }
  },
  "input": {
    "data": "=json.path(\"$.somepattern\", coerce.toObject($flow.dataobject))"
  }
}
```