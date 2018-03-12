# ESMySQLSync

A small library for syncing elastic search with mysql using [Zong Ji](https://github.com/nevill/zongji).

## Installation

    `npm install esmysqlsync`

## Usage

```javascript
const ESMySQLSync = require('./index');

const app = new ESMySQLSync({
  mysql: {
    host: 'localhost',
    user: 'slave',
    password: 'password',
  },
  index: ({ row }) => ({ index: 'products', type: 'product_type', id: row.id, body: row }),
  update: ({ row }) => ({ index: 'products', type: 'product_type', id: row.after.id, body: row.after }),
  delete: ({ row }) => ({ index: 'products', type: 'product_type', id: row.id }),
  success: res => console.log(res),
  error: err => console.log(err),
});

app.start({
  binlogName: 'mysql-log.000002',
  binlogNextPos: 7942,
}, () => console.log('Running'));

```