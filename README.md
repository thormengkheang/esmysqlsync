# ESMySQLSync

[![Build Status](https://travis-ci.org/mengkheang/esmysqlsync.svg?branch=master)](https://travis-ci.org/mengkheang/esmysqlsync)
[![Coverage Status](https://coveralls.io/repos/github/mengkheang/esmysqlsync/badge.svg?branch=master)](https://coveralls.io/github/mengkheang/esmysqlsync?branch=master)
[![Known Vulnerabilities](https://snyk.io/test/github/mengkheang/esmysqlsync/badge.svg)](https://snyk.io/test/github/mengkheang/esmysqlsync)

A small library for syncing elastic search with mysql using [Zong Ji](https://github.com/nevill/zongji).

## Installation

`npm install esmysqlsync`

## Usage

```javascript
const ESMySQLSync = require('./');

const app = new ESMySQLSync({
  mysql: {
    host: 'localhost',
    user: 'slave',
    password: 'password',
  },
  batch: 10, // default to 10
  index: ({ row, tableMap }) => {
    console.log(tableMap); // additional table data
    return { action: 'index', index: 'products', type: 'product_type', id: row.id, body: row };
  },
  update: ({ row }) => ({ action: 'update', index: 'products', type: 'product_type', id: row.after.id, body: row.after }),
  delete: ({ row }) => ({ action: 'delete', index: 'products', type: 'product_type', id: row.id }),
  success: res => console.log(res), // optional
  error: e => console.log(e), // optional
});

app.start({ startAtEnd: true });
console.log('Running');
```