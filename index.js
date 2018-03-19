const ZongJi = require('zongji');
const ElasticSearch = require('elasticsearch');
const ActionQueue = require('./lib/ActionQueue');

class ESMySQLSync {
  constructor({
    mysql, elastic, index, update, delete: remove,
    success = () => {}, error = () => {}, batch = 10,
  }) {
    this.zongJi = new ZongJi(mysql);
    this.elasticSearch = new ElasticSearch.Client({
      host: 'localhost:9200',
      log: 'trace',
      ...elastic,
    });
    this.index = index;
    this.update = update;
    this.delete = remove; // delete is a reserved keyword so have to alias to remove
    this.success = success;
    this.error = error;
    this.batch = batch;
    this.batchCounter = 0;
    this.bulkItems = [];
    this.queue = new ActionQueue();
  }

  stop() {
    this.zongJi.stop();
  }

  start(options) {
    this.zongJi.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      ...options,
    });
    this.listen();
  }

  setItemsAction(evt, handler) {
    const { rows } = evt;
    const tableMap = evt.tableMap[evt.tableId];
    rows.forEach((row) => {
      const result = handler({ row, tableMap });
      if (result === undefined) return;

      const { index: _index, type: _type, id: _id, body, action } = result;
      if (!action) {
        throw new Error('Elastic Search action not found');
      }

      this.batchCounter += 1;
      this.bulkItems.push({ [action]: { _index, _type, _id } });

      if (action === 'index') {
        this.bulkItems.push(body);
      }
      if (action === 'update') {
        this.bulkItems.push({ doc: body });
      }
    });
  }

  listen() {
    this.zongJi.on('binlog', (evt) => {
      const eventName = evt.getEventName();

      switch (eventName) {
        case 'writerows': {
          this.setItemsAction(evt, this.index);
          break;
        }
        case 'updaterows': {
          this.setItemsAction(evt, this.update);
          break;
        }
        case 'deleterows': {
          this.setItemsAction(evt, this.delete);
          break;
        }

        default:
          break;
      }

      if (this.batchCounter >= this.batch) {
        const body = this.bulkItems;
        this.bulkItems = [];
        this.batchCounter = 0;

        this.queue.run((next) => {
          this.elasticSearch.bulk({ body }).then((res) => {
            this.success(res);
            next();
          }).catch((err) => {
            this.error(err);
            next();
          });
        });
      }
    });
  }
}

module.exports = ESMySQLSync;
