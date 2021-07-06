# Changelog

## v0.0.5

### Bugfixes

  * [Cassandrax.Connection] Fixed >= and <= operators, which were backwards in
    the final query

## v0.0.4

### Documentation

  * Added examples using `where/2` and `get/2` with multiple filters

### Bugfixes

  * [Cassandrax.Keyspace] Allow passing multiple filters in one where clause
  * [Cassandrax.Query.Builder] Allow passing variables in queries (Thanks
    [rafbgarcia](https://github.com/loopsocial/cassandrax/pull/14))

### Tests

  * [Cassandrax.Keyspace] Improved readability by removing module attrs holding
    fixtures

## v0.0.3

### Documentation

  * Improved README documentation and examples

### Bugfixes

  * [Cassandrax.Keyspace] Fixed reads and batches not using default configs

## v0.0.2

### Documentation

  * Improved root level documentation (Thanks [@dogatunkay](https://github.com/dogatuncay))
  * Still missing module-specific pages for more details

### Bugfixes

  * [Cassandrax.Connection] Fixed support to multi-cluster setup
  * [Cassandrax.Keyspace] Fixed warning of callback not being implemented
  * [Cassandrax.KeyspaceTest] Fixed tests failing due to invalid CQL (Thanks
    [@dogatunkay](https://github.com/dogatuncay))

## v0.0.1

* Initial release
