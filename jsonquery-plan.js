var orStream = require('joiner-stream'),
    andStream = require('and-stream');

function keyfn(index) {
  return index.key[index.key.length - 1];
}

module.exports = optimizer;
function optimizer (query, plan) {
  return optimizerMatch(query, false, plan);
}

function optimizerMatch (predicate, negate, plan) {
  var tasks = [], idxStream;
  // { k: v, k2: v2 }
  // effectively an $and
  var sources = [];
  for (var n in predicate) {
    var v = predicate[n];
    if (v === undefined) {
      // do nothing
    } else if (n[0] === '$') {
      idxStream = optimizerOperator(n, v, negate, plan);
      if (idxStream) sources.push(idxStream);
    } else if (v.constructor === Object) {
      idxStream = optimizeValOpMatch(n, v, negate, plan);
      if (idxStream) sources.push(idxStream);
    } else {
      idxStream = plan(n, '$eq', v, negate);
      if (idxStream) sources.push(idxStream);
    }
  }

  if (sources.length === 0) return null;
  if (sources.length === 1) return sources[0];
  var and = andStream(keyfn);
  for (var i = 0; i < sources.length; i++) {
    var source = sources[i];
    source.pipe(and.stream());
  }
  return and;
}

function optimizerOperator (op, predicate, negate, plan) {
  var i, task, idxStream;
  switch (op) {
    case '$or':
    case '$and':
      if (negate) {
        if (op === '$or') op = '$and';
        else op = '$or';
      }
      var opStream = (op === '$or') ?
        orStream() : andStream(keyfn);
      var sources = [];
      for (i = 0, len = predicate.length; i < len; i++) {
        var part = predicate[i];
        idxStream = optimizerMatch(part, negate, plan);
        if (idxStream) sources.push(idxStream);
      }
      if (sources.length === 0) return null;
      if (sources.length === 1) return sources[0];
      for (i = 0; i < sources.length; i++) {
        var source = sources[i];
        source.pipe(op === '$or' ? opStream : opStream.stream());
      }
      return opStream;

    case '$not':
      return optimizerMatch(predicate, !negate, plan);
  }
}

function optimizeValOpMatch (val, predicate, negate, plan) {
  // { $in: [], $gt : 3 }
  // effectively an $and
  var idxStream, sources = [];
  for (var n in predicate) {
    var v = predicate[n];
    if (n[0] === '$') {
      idxStream = optimizeValOp(n, val, v, negate, plan);
      if (idxStream) sources.push(idxStream);
    }
  }
  if (sources.length === 0) return null;
  if (sources.length === 1) return sources[0];
  var and = andStream(keyfn);
  for (var i = 0; i < sources.length; i++) {
    var source = sources[i];
    source.pipe(and.stream());
  }
  return and;
}

function optimizeValOp (op, val, args, negate, plan) {
  var part, task, tasks, i, path, arg, and, stream, sources, source, idxStream;
  switch (op) {
    case '$in':
      sources = [];
      if (Array.isArray(args)) {
        for (i = 0; i < args.length; i++) {
          arg = args[i];
          idxStream = plan(val, '$eq', arg, negate);
          if (idxStream) sources.push(idxStream);
        }
      }
      if (sources.length === 0) return null;
      if (sources.length === 1) return sources[0];
      var or = orStream();
      for (i = 0; i < sources.length; i++) {
        source = sources[i];
        source.pipe(or);
      }
      return or;

    case '$nin':
      return plan(val, args, args, !negate);

    case '$gt':
    case '$gte':
    case '$ne':
    case '$lt':
    case '$lte':
    case '$mod':
      return plan(val, op, args, negate);

    case '$all': // { favorites: { $all: [50, 60] } }
      sources = [];
      for (i = 0, len = args.length; i < len; i++) {
        arg = args[i];
        idxStream = plan(val, '$eq', arg, negate);
        if (idxStream) sources.push(idxStream);
      }
      if (sources.length === 0) return null;
      if (sources.length === 1) return sources[0];
      and = andStream(keyfn);
      for (i = 0; i < sources.length; i++) {
        source = sources[i];
        source.pipe(and.stream());
      }
      return and;

    case '$elemMatch':
      sources = [];
      for (part in args) {
        var v = args[part];
        var key = [val, part].join('.');
        var p = {};
        p[key] = v;
        idxStream = optimizerMatch (p, negate, plan);
        if (idxStream) sources.push(optimizerMatch (p, negate, plan));
      }
      if (sources.length === 0) return null;
      if (sources.length === 1) return sources[0];
      and = andStream(keyfn);
      for (i = 0; i < sources.length; i++) {
        source = sources[i];
        source.pipe(and.stream());
      }
      return and;

    case '$or':
    case '$and':
      if (negate) {
        if (op === '$or') op = '$and';
        else op = '$or';
      }
      sources = [];
      for (i = 0, len = args.length; i < len; i++) {
        arg = args[i];
        idxStream = optimizeValOpMatch(val, arg, negate, plan);
        if (idxStream) sources.push(idxStream);
      }
      if (sources.length === 0) return null;
      if (sources.length === 1) return sources[0];
      var opStream = (op === '$or') ?
        orStream() : andStream(keyfn);
      for (i = 0; i < sources.length; i++) {
        source = sources[i];
        source.pipe(op === '$or' ? opStream : opStream.stream());
      }
      return opStream;

    case '$not':
      return optimizeValOpMatch(val, args, !negate, plan);
  }
}
