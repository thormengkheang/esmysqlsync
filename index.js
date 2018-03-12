const ZongJi = require('zongji');
const ElasticSearch = require('elasticsearch');


const setItemsAction = (rows, action, handler) => {
  const bulkItems = [];
  rows.forEach((row) => {
    const { index: _index, type: _type, id: _id, body } = handler({ row });
    bulkItems.push({ [action]: { _index, _type, _id } });
    if (action === 'index') {
      bulkItems.push(body);
    }
    if (action === 'update') {
      bulkItems.push({ doc: body });
    }
  });
  return bulkItems;
};

class ESMySQLSync {
  constructor({ mysql, elastic, index, update, delete: remove, success, error }) {
    this.zongJi = new ZongJi(mysql);
    this.elasticSearch = new ElasticSearch.Client({
      host: 'localhost:9200',
      ...elastic,
    });
    this.index = index;
    this.update = update;
    this.delete = remove;
    this.success = success;
    this.error = error;
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
    callback();
  }

  listen() {
    this.zongJi.on('binlog', (evt) => {
      const eventName = evt.getEventName();
      let bulkItems = [];

      switch (eventName) {
        case 'writerows':
          bulkItems = setItemsAction(evt.rows, 'index', this.index);
          break;
        case 'updaterows':
          bulkItems = setItemsAction(evt.rows, 'update', this.update);
          break;
        case 'deleterows':
          bulkItems = setItemsAction(evt.rows, 'delete', this.delete);
          break;

        default:
          break;
      }

      if (bulkItems.length > 0) {
        this.elasticSearch.bulk({ body: bulkItems })
          .then(res => this.success(res))
          .catch(err => this.error(err));
      }
    });
  }
}

module.exports = ESMySQLSync;