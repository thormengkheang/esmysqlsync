module.exports = class ActionQueue {
  constructor() {
    this.capacity = 1024;
    this.start = 0;
    this.end = 0;
    this.size = 0;
    this.queue = new Array(this.capacity);
  }

  run(task) {
    this.size += 1;
    this.queue[this.end] = task;
    this.end = (this.end + 1) % this.capacity;

    const next = () => {
      this.size -= 1;
      this.start = (this.start + 1) % this.capacity;

      if (this.size > 0) this.queue[this.start](next);
    };

    if (this.size === 1) task(next);
  }
};
