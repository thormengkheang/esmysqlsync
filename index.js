const ZongJi = require('zongji');
const ElasticSearch = require('elasticsearch');

class ESMySQLSync {
  constructor({
    mysql, elastic, index, update, delete: remove,
    success = () => {}, error = () => {}, batch = 10,
  }) {
    this.zongJi = new ZongJi(mysql);
    this.elasticSearch = new ElasticSearch.Client({
      host: 'localhost:9200',
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
  }

  stop()
  {
    this.zongJi.stop();
  }

  start(options, callback) {
    this.zongJi.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      ...options,
    });
    this.listen();
    if (typeof callback === 'function') {
      callback();
    }
  }

  setItemsAction(evt, handler) {
    const { rows } = evt;
    const tableMap = evt.tableMap[evt.tableId];
    rows.forEach((row) => {
      const { index: _index, type: _type, id: _id, body, action } = handler({ row, tableMap });
      if (!action) {
        throw new Error('Elastic Search action not found');
      }

      this.batchCounter++;
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
        let t = this.bulkItems;
        this.bulkItems = [];
        this.batchCounter = 0;
        console.log(t);

        this.elasticSearch.bulk({ body: t })
          .then((res) => {
            this.success(res);
            console.log("Success", res);
          })
          .catch(err => this.error(err));
      }
    });
  }
}

module.exports = ESMySQLSync;