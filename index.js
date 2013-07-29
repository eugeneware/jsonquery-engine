var jsonquery = require('jsonquery'),
    jsonqueryPlan = require('./jsonquery-plan'),
    through = require('through');

module.exports = jsonqueryEngine;
function jsonqueryEngine() {
  return {
    query: query,
    match: jsonquery.match,
    plans: {
      'property': propertyPlan,
      'pairs': pairsPlan
    }
  };
}

function keyfn(index) {
  return index.key[index.key.length - 1];
}

function valfn(index) {
  return index.key[index.key.length - 2];
}

function propertyPlan(idx, prop, op, value, negate) {
  var db = this;
  var ops = {
     $gt: function (a, b) { return a > b; },
    $gte: function (a, b) { return a >= b; },
     $lt: function (a, b) { return a < b; },
    $lte: function (a, b) { return a <= b; },
    $mod: function (a, b) { return a % b[0] === b[1]; }
  };

  var negOps = {
     $gt: '$lte',
    $gte: '$lt',
     $lt: '$gte',
    $lte: '$gt'
  };

  if (negate) {
    if (op in negOps) {
      // transform to negated equivalent
      op = negOps[op];
      // don't negate predicate as transform already done
      negate = !negate;
    }
  }

  function indexFilterStream() {
    return through(function (data) {
      var val = valfn(data);
      if (ops[op](val, value) === (!negate)) {
        this.queue(data);
      }
    });
  }

  switch (op) {
    case '$eq':
      if (value instanceof RegExp) {
        return db.indexes[idx].createIndexStream()
          .pipe(through(function (data) {
            var val = valfn(data);
            if (value.test(val)) {
              this.queue(data);
            }
          }));
      } else {
        return db.indexes[idx].createIndexStream({
          start: [value, null],
          end: [value, undefined]
        });
      }
      break;

    case '$gt':
    case '$gte':
      return db.indexes[idx].createIndexStream({
        start: [value, null],
        end: [undefined, undefined]
      })
      .pipe(indexFilterStream());

    case '$lt':
    case '$lte':
      return db.indexes[idx].createIndexStream({
        start: [null, null],
        end: [value, undefined]
      })
      .pipe(indexFilterStream());

    case '$mod':
      return db.indexes[idx].createIndexStream()
      .pipe(indexFilterStream());
  }

  return null;
}

function takeTwo(path) {
  if (path.length > 2) return path.slice(-2);
  return path;
}

function pairsPlan(idx, prop, op, value, negate) {
  var db = this, path;
  var ops = {
     $gt: function (a, b) { return a > b; },
    $gte: function (a, b) { return a >= b; },
     $lt: function (a, b) { return a < b; },
    $lte: function (a, b) { return a <= b; },
    $mod: function (a, b) { return a % b[0] === b[1]; }
  };

  var negOps = {
     $gt: '$lte',
    $gte: '$lt',
     $lt: '$gte',
    $lte: '$gt'
  };

  if (negate) {
    if (op in negOps) {
      // transform to negated equivalent
      op = negOps[op];
      // don't negate predicate as transform already done
      negate = !negate;
    }
  }

  function indexFilterStream() {
    return through(function (data) {
      var val = valfn(data);
      if (ops[op](val, value) === (!negate)) {
        this.queue(data);
      }
    });
  }

  if (prop) {
    path = prop.split('.');
  }

  switch (op) {
    case '$eq':
      if (value instanceof RegExp) {
        return db.indexes[idx].createIndexStream()
          .pipe(through(function (data) {
            var val = valfn(data);
            if (value.test(val)) {
              this.queue(data);
            }
          }));
      } else {
        return db.indexes[idx].createIndexStream({
          start: takeTwo(path.concat(value)).concat(null),
          end: takeTwo(path.concat(value)).concat(undefined)
        });
      }
      break;

    case '$gt':
    case '$gte':
      return db.indexes[idx].createIndexStream({
        start: takeTwo(path.concat(value)).concat(null),
        end: takeTwo(path.concat(undefined)).concat(undefined)
      })
      .pipe(indexFilterStream());

    case '$lt':
    case '$lte':
      return db.indexes[idx].createIndexStream({
        start: takeTwo(path.concat(null)).concat(null),
        end: takeTwo(path.concat(value)).concat(undefined)
      })
      .pipe(indexFilterStream());

    case '$mod':
      // the index size will be comparable with the data read, just table scan
      return null;
  }

  return null;
}

function plan(prop, op, value, negate) {
  var db = this;
  var idx = db.indexes[prop];
  if (idx && idx.type in db.query.engine.plans) {
    return db.query.engine.plans[idx.type].call(db, prop, prop, op, value, negate);
  } else if ((idx = db.indexes['*']) && idx.type in db.query.engine.plans) {
    return db.query.engine.plans[idx.type].call(db, '*', prop, op, value, negate);
  } else {
    return null;
  }
}

function query(q) {
  var db = this;
  return jsonqueryPlan(q, plan.bind(db));
}
