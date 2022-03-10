module.exports = class AsyncQueue {
  internalList = [];


  constructor({ backpressureMax = Infinity }) {
    this.backpressureMax = backpressureMax;
    this.addPromise = new Promise(resolve => {
      this.addResolve = resolve;
    });
    this.removePromise = new Promise(resolve => {
      this.removeResolve = resolve;
    });
  }


  async push(...values) {
    this.internalList.push(...values);
    while (this.backpressureMax < this.internalList.length) {
      await this.removePromise;
    }
    this.noticeAdd();
    return this;
  }

  async take(max = -1) {
    while (this.internalList.length <= 0) {
      await this.addPromise;
    }
    const toReturn = max === -1 ? this.internalList.shift() : this.internalList.splice(0, max);
    this.noticeRemove();
    return toReturn;
  }

  empty() {
    this.internalList.length === 0;
  }

  noticeRemove() {
    const toResolve = this.removeResolve;
    this.removePromise = new Promise(resolve => {
      this.removeResolve = resolve;
    });
    setTimeout(toResolve, 0);
  }

  noticeAdd() {
    const toResolve = this.addResolve;
    this.addPromise = new Promise(resolve => {
      this.addResolve = resolve;
    });
    setTimeout(toResolve, 0);
  }

}