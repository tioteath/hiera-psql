[![Build Status](https://travis-ci.org/tioteath/hiera-psql.svg?branch=master)](https://travis-ci.org/tioteath/hiera-psql)

Database schema
===============

Please note: you need PostgreSQL at least version 9.3 in order to use this 
backend. Use v.0.1.0 for older PostgreSQL.

The database should contain a table 'data' with two text columns (environment
 and path), and one json column (value).
Environment is an enviroment from ``hiera.yaml``. Path is equivalent to the 
path in the hierarchy (with no file extensions) and value should contain the value in JSON format.

Example:
```
| environment    | path                   | value                                                  |
|:---------------|:-----------------------|:-------------------------------------------------------|
| 'production'   | 'fqdn/foo.example.com' | {"class::num_param": 42, "class::str_param": "foobar"} |
| 'production'   | 'fqdn/bar.example.com' | {"class::array_param": [1, 2, 3]}                      |
| 'test'         | 'fqdn/baz.example.com' | {"class::hash_param": { "key1": "value1", "key2": 2 }} |
```

SQL:
```
    CREATE DATABASE hiera WITH owner=hiera template=template0 encoding='utf8';
    CREATE TABLE data (environment text, path text, value json);
    CREATE UNIQUE INDEX on data (path, environment);
```

Configuration
=============

The backend configuration takes a connection hash that it sends directly to the connect method of the postgres library. See the [ruby-pg documentation](http://deveiate.org/code/pg/PG/Connection.html#method-c-new) for more info on parameters it accepts.

Here is a example hiera config file:
```
    ---
    :hierarchy:
      - 'fqdn/%{fqdn}'
      - common
    
    :backends:
      - psql
    
    :psql:
      :environment: production
      :connection:
        :dbname: hiera
        :host: localhost
        :user: hiera
        :password: hiera
        
```

If no environment provided in configuration file, then it will not be 
included in query condition, and simple query will be used instead.
For example, configuration above will result in:
```
SELECT value->'example_value' FROM data WHERE environment='production'  AND 
path='/example_path'
```
While if environment is not set, it evaluates in:
```
SELECT value->'example_value' FROM data WHERE path='/example_path'
```
