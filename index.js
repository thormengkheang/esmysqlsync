const ZongJi = require('zongji');
const ElasticSearch = require('elasticsearch');

class ESMySQLSync {
  constructor({
    mysql, elastic, index, update, delete: remove,
    success = () => {}, error = () => {}, smallestBatch = 10,
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
    this.smallestBatch = smallestBatch;
    this.batchCount = 0;
    this.bulkItems = [];
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
      const { index: _index, type: _type, id: _id, body, action } = handler({ row, tableMap });
      if (!action) {
        throw new Error('Elastic Search action not found');
      }
      this.bulkItems.push({ [action]: { _index, _type, _id } });
      if (action === 'index') {
        this.bulkItems.push(body);
      }
      if (action === 'update') {
        this.bulkItems.push({ doc: body });
      }
      this.batchCount += 1;
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
          evt.dump();
      }

      if (this.bulkItems.length > 0 && this.batchCount >= this.smallestBatch) {
        this.elasticSearch.bulk({ body: this.bulkItems })
          .then((res) => {
            this.success(res);
            this.bulkItems = [];
            this.batchCount = 0;
          })
          .catch(err => this.error(err));
      }
    });
  }
}

module.exports = ESMySQLSync;
