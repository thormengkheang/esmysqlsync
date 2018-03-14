const mysql = require('mysql');
const ElasticSearch = require('elasticsearch');
const ESMySQLSync = require('../');
const config = require('./config');

test('test sync between mysql and elastic search', async () => {
  const connection = mysql.createConnection({
    host: config.mysql_host,
    user: config.mysql_user,
    password: config.mysql_pass,
    database: config.mysql_db,
    port: config.mysql_port,
  });

  // Clearing up mysql data
  await connection.connect();
  await connection.query(`CREATE TABLE IF NOT EXISTS test_user (
    id INT(11) NOT NULL,
    name VARCHAR(200) NOT NULL,
    gender ENUM('F','M') NOT NULL,
    title VARCHAR(200) NOT NULL,
    salary INT(11) NOT NULL,
    PRIMARY KEY (id)
  )`);
  await connection.query('DELETE FROM mysql_es_test.test_user');

  // Clearing up elastic search data
  const es = new ElasticSearch.Client({ host: config.es_host });
  await es.deleteByQuery({ index: 'test_user', type: 'user', body: { query: { match_all: { } } } }).catch(() => { });

  // Start listen for MySQL changes
  const s = new ESMySQLSync({
    mysql: {
      host: config.mysql_host,
      user: config.mysql_user,
      password: config.mysql_pass,
      port: config.mysql_port,
    },
    elastic: { host: config.es_host },
    batch: 1,
    index: ({ row }) => ({ action: 'index', index: 'test_user', type: 'user', id: row.id, body: row }),
    update: ({ row }) => ({ action: 'update', index: 'test_user', type: 'user', id: row.after.id, body: row.after }),
    delete: ({ row }) => ({ action: 'delete', index: 'test_user', type: 'user', id: row.id }),
    success: () => { },
    error: () => { },
  });

  s.start({ startAtEnd: true }, () => { });

  // Wait 2 seconds for our engine to properly start
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Let make some changes
  await connection.query('INSERT INTO test_user VALUES(1, "Jonh", "M", "CEO", 1200)');
  await connection.query('INSERT INTO test_user VALUES(2, "Mike", "M", "CTO", 1000)');
  await connection.query('INSERT INTO test_user VALUES(4, "Jenny", "F", "CFO", 500)');
  await connection.query('UPDATE test_user SET salary = salary + 100 WHERE salary >= 1000');
  await connection.query('INSERT INTO test_user VALUES(5, "Denny", "F", "Accounting", 500)');
  await connection.query('INSERT INTO test_user VALUES(6, "Sopheak", "M", "Programmer", 1000)');
  await connection.query('DELETE FROM test_user WHERE salary < 600');
  await connection.end();

  // Give our engine 5 seconds chance to sync
  // from MySQL to Elastic Search
  await new Promise((resolve) => {
    setTimeout(() => {
      s.stop();
      resolve();
    }, 5000);
  });

  // check the result from elastic search
  const r1 = await es.get({ index: 'test_user', type: 'user', id: 1 }).catch(() => {});
  const r2 = await es.get({ index: 'test_user', type: 'user', id: 2 }).catch(() => {});
  const r3 = await es.get({ index: 'test_user', type: 'user', id: 6 }).catch(() => {});
  const r4 = await es.get({ index: 'test_user', type: 'user', id: 4 }).catch(() => {});
  const r5 = await es.get({ index: 'test_user', type: 'user', id: 5 }).catch(() => {});

  expect(r1).not.toBe(undefined);
  expect(r2).not.toBe(undefined);
  expect(r3).not.toBe(undefined);
  expect(r4).toBe(undefined);
  expect(r5).toBe(undefined);

  expect(r1._source).toEqual({ id: 1, name: 'Jonh', gender: 'M', title: 'CEO', salary: 1300 });
  expect(r2._source).toEqual({ id: 2, name: 'Mike', gender: 'M', title: 'CTO', salary: 1100 });
  expect(r3._source).toEqual({ id: 6, name: 'Sopheak', gender: 'M', title: 'Programmer', salary: 1000 });

}, 15000);
