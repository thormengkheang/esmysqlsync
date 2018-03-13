# ESMySQLSync

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
  smallestBatch: 1, // default to 10
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