var expect = require('chai').expect,
    bytewise = require('bytewise'),
    levelup = require('levelup'),
    path = require('path'),
    pairs = require('pairs'),
    levelQuery = require('level-queryengine'),
    jsonqueryEngine = require('../index'),
    rimraf = require('rimraf'),
    after = require('after');

function encode(key) {
  return bytewise.encode(key).toString('hex');
}

function decode(key) {
  return bytewise.decode(new Buffer(key, 'hex'));
}

function log() {
  console.error.apply(console, [].slice.apply(arguments));
}

describe('level-plan', function() {
  var db, dbPath = path.join(__dirname, '..', 'data', 'test-db');

  beforeEach(function(done) {
    rimraf.sync(dbPath);
    db = levelup(dbPath, { valueEncoding: 'json' }, done);
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should be able to do jsonqueries with a property index', function(done) {
    var tests = [
      {
           query: { 'name': 'name 42' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 },
      },
      {
           query: { $or: [ { num: 420 }, { name: 'name 42' } ] },
        expected: { indexHits: 2, dataHits: 1, matchHits: 1 }
      },
      {
        query: { $and: [ { tags: 'tag1' }, { num: { $lt: 100 } } ] },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { $and: [ { tagsNoIndex: 'tag1' }, { num: { $lt: 100 } } ] },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { tags: 'tag4' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { tagsNoIndex: 'tag4' },
        expected: { indexHits: 0, dataHits: 100, matchHits: 1 }
      },
      {
           query: { 'numNoIndex': 420 },
        expected: { indexHits: 0, dataHits: 100, matchHits: 1 }
      },
      {
           query: { 'name': { $ne: 'name 42' } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 99 },
      },
      {
           query: { 'num': { $gte: 500 } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { $not: { 'num': { $gte: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $not: { $gte: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $gt: 500 } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { $not: { 'num': { $gt: 500 } } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { 'num': { $not: { $gt: 500 } } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { 'num': { $lt: 500 } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { $not: { 'num': { $lt: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $not: { $lt: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $lte: 500 } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { $not: { 'num': { $lte: 500 } } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { 'num': { $not: { $lte: 500 } } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { num: 420, name: 'name 42' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { num: { $mod: [200, 0] } },
        expected: { indexHits: 5, dataHits: 5, matchHits: 5 }
      },
      {
           query: { $not: { num: { $mod: [200, 0] } } },
        expected: { indexHits: 95, dataHits: 95, matchHits: 95 }
      },
      {
           query: { num: { $in: [420, 70] } },
        expected: { indexHits: 2, dataHits: 2, matchHits: 2 }
      },
      {
           query: { num: { $nin: [420, 70] } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 98 }
      },
      {
           query: { tags: { $all: ['tag2', 'tag4'] } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { tagsNoIndex: { $all: ['tag2', 'tag4'] } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 1 }
      },
      {
        query: { treeNoIndex: { $elemMatch: { a: 42, b: 43 } } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 1 }
      },
      {
        query: { tree: { $elemMatch: { a: 42, b: 43 } } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { num: { $or: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 111, dataHits: 100, matchHits: 100 }
      },
      {
        query: { num: { $and: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
        query: { $not: { num: { $or: [ { $lt: 100 }, { $gt: 200 } ] } } },
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
        query: { numNoIndex: { $and: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 0, dataHits: 100, matchHits: 11 }
      },
      {
        query: { num: { $not: { $gte: 100 } } },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { numNoIndex: { $not: { $gte: 100 } } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 10 }
      },
      {
        query: { 'tree.a': 42 },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { 'tree.b': 43 },
        expected: { indexHits: 0, dataHits: 100, matchHits: 1 }
      },
      {
           query: { 'name': /^name 42$/ },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { 'name': /^name 4/ },
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
           query: { 'nothing': undefined },
        expected: { indexHits: 0, dataHits: 100, matchHits: 100 }
      },
    ];

    db = levelQuery(db);

    db.query.use(jsonqueryEngine());
    db.ensureIndex('name');
    db.ensureIndex('num');
    db.ensureIndex('tags');
    db.ensureIndex('tree.a');

    db.batch(testData(), doQueries);

    function doQueries(err) {
      if (err) return done(err);
      var candidateTests = tests.filter(function (test) {
        return test.debug === true || test.only === true;
      });
      if (candidateTests.length) tests = candidateTests;
      var next = after(tests.length, done);

      tests.forEach(function (test) {
        if (test.before) {
          test.before(doWork);
        } else {
          doWork();
        }

        function doWork() {
          var hits = 0;
          db.query(test.query)
            .on('data', function (data) {
              if (test.debug) console.error(data);
              hits++;
            })
            .on('stats', function (stats) {
              if (test.debug) console.error(stats);
              expect(stats).to.deep.equals(test.expected);
            })
            .on('end', function () {
              expect(hits).to.equal(test.expected.matchHits);
              next();
            });
        }
      });
    }
  });

  it('should be able to do jsonqueries with a pairs index', function(done) {
    var tests = [
      {
           query: { 'name': 'name 42' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 },
      },
      {
           query: { $or: [ { num: 420 }, { name: 'name 42' } ] },
        expected: { indexHits: 2, dataHits: 1, matchHits: 1 }
      },
      {
        query: { $and: [ { tags: 'tag1' }, { num: { $lt: 100 } } ] },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { $and: [ { tagsNoIndex: 'tag1' }, { num: { $lt: 100 } } ] },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { tags: 'tag4' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { tagsNoIndex: 'tag4' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { 'numNoIndex': 420 },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { 'name': { $ne: 'name 42' } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 99 },
      },
      {
           query: { 'num': { $gte: 500 } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { $not: { 'num': { $gte: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $not: { $gte: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $gt: 500 } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { $not: { 'num': { $gt: 500 } } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { 'num': { $not: { $gt: 500 } } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { 'num': { $lt: 500 } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { $not: { 'num': { $lt: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $not: { $lt: 500 } } },
        expected: { indexHits: 50, dataHits: 50, matchHits: 50 }
      },
      {
           query: { 'num': { $lte: 500 } },
        expected: { indexHits: 51, dataHits: 51, matchHits: 51 }
      },
      {
           query: { $not: { 'num': { $lte: 500 } } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { 'num': { $not: { $lte: 500 } } },
        expected: { indexHits: 49, dataHits: 49, matchHits: 49 }
      },
      {
           query: { num: 420, name: 'name 42' },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { num: { $mod: [200, 0] } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 5 }
      },
      {
           query: { $not: { num: { $mod: [200, 0] } } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 95 }
      },
      {
           query: { num: { $in: [420, 70] } },
        expected: { indexHits: 2, dataHits: 2, matchHits: 2 }
      },
      {
           query: { num: { $nin: [420, 70] } },
        expected: { indexHits: 0, dataHits: 100, matchHits: 98 }
      },
      {
           query: { tags: { $all: ['tag2', 'tag4'] } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { tagsNoIndex: { $all: ['tag2', 'tag4'] } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { treeNoIndex: { $elemMatch: { a: 42, b: 43 } } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { tree: { $elemMatch: { a: 42, b: 43 } } },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { num: { $or: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 111, dataHits: 100, matchHits: 100 }
      },
      {
        query: { num: { $and: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
        query: { $not: { num: { $or: [ { $lt: 100 }, { $gt: 200 } ] } } },
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
        query: { numNoIndex: { $and: [ { $gte: 100 }, { $lte: 200 } ] }},
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
        query: { num: { $not: { $gte: 100 } } },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { numNoIndex: { $not: { $gte: 100 } } },
        expected: { indexHits: 10, dataHits: 10, matchHits: 10 }
      },
      {
        query: { 'tree.a': 42 },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
        query: { 'tree.b': 43 },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { 'name': /^name 42$/ },
        expected: { indexHits: 1, dataHits: 1, matchHits: 1 }
      },
      {
           query: { 'name': /^name 4/ },
        expected: { indexHits: 11, dataHits: 11, matchHits: 11 }
      },
      {
           query: { 'nothing': undefined },
        expected: { indexHits: 0, dataHits: 100, matchHits: 100 }
      },
    ];

    db = levelQuery(db);

    db.query.use(jsonqueryEngine());
    db.ensureIndex('*', 'pairs', pairs.index);

    db.batch(testData(), doQueries);

    function doQueries(err) {
      if (err) return done(err);
      var candidateTests = tests.filter(function (test) {
        return test.debug === true || test.only === true;
      });
      if (candidateTests.length) tests = candidateTests;
      var next = after(tests.length, done);

      tests.forEach(function (test) {
        if (test.before) {
          test.before(doWork);
        } else {
          doWork();
        }

        function doWork() {
          var hits = 0;
          db.query(test.query)
            .on('data', function (data) {
              if (test.debug) console.error(data);
              hits++;
            })
            .on('stats', function (stats) {
              if (test.debug) console.error(stats);
              expect(stats).to.deep.equals(test.expected);
            })
            .on('end', function () {
              expect(hits).to.equal(test.expected.matchHits);
              next();
            });
        }
      });
    }
  });
});

function testData() {
  var batch = [];
  for (var i = 0; i < 100; i++) {
    var obj = {
      name: 'name ' + i,
      car: {
        make: 'Toyota',
        model: i % 2 ? 'Camry' : 'Corolla',
        year: 1993 + i
      },
      pets: [
        { species: 'Cat', breed: i == 50 ? 'Saimese' : 'Burmese' },
        { species: 'Cat', breed: 'DSH' },
        { species: 'Dog', breed: 'Dalmation' }
      ],
      tags: [
        'tag1', 'tag2', 'tag3'
      ],
      tagsNoIndex: [
        'tag1', 'tag2', 'tag3'
      ],
      nothing: undefined,
      tree: {
        a: i,
        b: i + 1,
      },
      treeNoIndex: {
        a: i,
        b: i + 1,
      },
      num: 10*i,
      numNoIndex: 10*i
    };
    if (i === 42) {
      obj.tags.push('tag4');
      obj.tagsNoIndex.push('tag4');
    }
    if (i === 84) {
      delete obj.name;
    }
    batch.push({ type: 'put', key: i, value: obj });
  }

  return batch;
}
