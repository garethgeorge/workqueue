export default class EventEmitter<T> {
  private listeners: ((event: T) => any)[] | null = null;

  listen(listener: (event: T) => any) {
    if (!this.listeners) this.listeners = [];
    this.listeners.push(listener);
    return listener;
  }

  removeListener(listener) {
    if (!this.listeners) return;
    let idx = this.listeners.indexOf(listener);
    if (idx !== -1) {
      this.listeners.splice(idx, 1);
    }
  }

  emit(event: T) {
    if (!this.listeners) return;
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}
