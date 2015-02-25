# jsonquery-engine

A full [MongoDB query language](http://docs.mongodb.org/manual/reference/operator/)
implementation **with INDEXES** for querying your levelup/leveldb database.

This is a plugin for [level-queryengine](https://github.com/eugeneware/level-queryengine).

[![build status](https://secure.travis-ci.org/eugeneware/jsonquery-engine.png)](http://travis-ci.org/eugeneware/jsonquery-engine)

# Installation

Install through npm:

```
$ npm install jsonquery-engine
```

# Usage

``` js
var levelQuery = require('level-queryengine'),
    jsonqqueryEngine = require('jsonquery-engine'),
    pairs = require('pairs'),
    levelup = require('levelup'),
    db = levelQuery(levelup('my-db', { valueEncoding: 'json' }));

db.query.use(jsonqueryEngine());

// index all the properties in pairs
db.ensureIndex('*', 'pairs', pairs.index);

// alternatively you could just index the properties you want:
// db.ensureIndex('num');
// db.ensureIndex('tags');

db.batch(makeSomeData(), function (err) {
  // compound mongodb / jsonquery query syntax
  db.query({ $and: [ { tags: 'tag1' }, { num: { $lt: 100 } } ] })
    .on('data', console.log)
    .on('stats', function (stats) {
      // stats contains the query statistics in the format
      //  { indexHits: 1, dataHits: 1, matchHits: 1 });
    });
});
```

## Example Queries

I'm using my [jsonquery](https://github.com/eugeneware/jsonquery) module to
implement that final, ultimate mongodb syntax.

This module adds awesome **INDEX** support to the syntax, so you're not just
filtering your entire database stream, but taking advantage of any indexes
that are set up using [level-queryengine](https://github.com/eugeneware/level-queryengine)

Here are some sample queries from the test suite. They all will take advantage
of any indexes for filtering **before** looking up values.

``` js
// will use indexes for quick retrieval if present
{ 'name': 'name 42' }

// if both fields are present, then indexes will be used before hitting values
{ $or: [ { num: 420 }, { name: 'name 42' } ] }

// $ands are smart so that if one of the fields is indexed, that will be used for retrieval
{ $and: [ { tags: 'tag1' }, { num: { $lt: 100 } } ] }

// can search efficiently for items in array. eg: { tags: [ 'tag1', 'tag4' ] }
{ tags: 'tag4' }

// will still require a full index scan, but depending on your data it won't need to do a full db scan
{ 'name': { $ne: 'name 42' } }

// smart enough to use levelups sorted indexes to efficiently do range queries BEFORE fetching data
{ 'num': { $gte: 500 } }

// smart enough to turn these both into { 'num': { $lte: 500 } } and use and index range lookup
{ $not: { 'num': { $gte: 500 } } }
{ 'num': { $not: { $gte: 500 } } }

// index scan
{ num: { $mod: [200, 0] } }

// will use indexes
{ num: { $in: [420, 70] } }

// $nins suck - table scan
{ num: { $nin: [420, 70] } }

// will use indexes for efficient retrieval
{ tags: { $all: ['tag2', 'tag4'] } }

// will use indexes for efficient retrieval
{ tree: { $elemMatch: { a: 42, b: 43 } } }

// will use indexes for efficient retrieval
{ 'tree.a': 42 }

// index scan
{ 'name': /^name 4/ }
```

# Indexing Strategy Support

Currently two index strategies are supported:

* `'property'` (default) - index the property defined by the `indexName`.
  If you don't pass in any `emitFunction` (or `indexType`) then this indexing
  strategy will be used by default.
* `'pairs'` - used by the [pairs](https://github.com/eugeneware/pairs) module
   and [jsonquery-engine](https://github.com/eugeneware/jsonquery-engine) to
   index "pairs" of object properties to allow arbitrary object queries with
   a reasonable tradeoff between index size and query performance.

To use the alacarte `'property'` system:

``` js
db.query.use(jsonqueryEngine());

// index these properties
db.ensureIndex('num');
db.ensureIndex('tree.a');

db.query(...);
```

To use the `'pairs'` strategy, which effectively indexes almost EVERY field,
with a nice balance between selectiveness and index size:

``` js
var pairs = require('pairs');
db.query.use(jsonqueryEngine());

// index all pairs of properties
db.ensureIndex('*', 'pairs', pairs.index);

db.query(...);
```

This will enable you to do effective ad-hoc queries on practically any field.
But, be aware the pairs indexing can be VERY large.

# TODO

This project is under active development. Here's a list of things I'm planning to add:

* There are still some bugs in the jsonquery query syntax. Eg: { name: { first: 'bob' } } doens't currently work properly.
* support the `'full-path'` indexing strategy.
* joins?
