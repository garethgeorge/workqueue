export default class EventEmitter<T> {
  private listeners: ((event: T) => any)[] = [];

  listen(listener: (event: T) => any) {
    this.listeners.push(listener);
  }

  removeListener(listener) {
    let idx = this.listeners.indexOf(listener);
    if (idx !== -1) {
      this.listeners.splice(idx, 1);
    }
  }

  emit(event: T) {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}
