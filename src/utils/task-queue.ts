interface Task<TResult = any> {
  execute(): Promise<TResult>;
  onerror?(e: unknown): Promise<void>;
}

class TaskNode {
  constructor(public task: Task, public next?: TaskNode) {}
}

export class TaskQueue {
  protected head?: TaskNode;
  protected tail?: TaskNode;
  protected running: boolean;

  constructor() {
    this.running = false;
  }

  public push(task: Task) {
    if (!this.head) {
      this.head = this.tail = new TaskNode(task);
    } else {
      this.tail!.next = new TaskNode(task);
      this.tail = this.tail!.next;
    }
    this.run();
  }

  protected pop() {
    if (!this.head) {
      return undefined;
    }
    const task = this.head.task;
    if (this.head === this.tail) {
      this.head = this.tail = undefined;
    } else {
      const next = this.head.next;
      this.head.next = undefined;
      this.head = next;
    }
    return task;
  }

  protected async run() {
    if (this.running) {
      return;
    }
    this.running = true;
    for (let task = this.pop(); task != null; task = this.pop()) {
      try {
        await task.execute();
      } catch (error) {
        await task.onerror?.(error);
      }
    }
    this.running = false;
  }

  public clear() {
    let node = this.head;
    while (node) {
      const next = node.next;
      node.next = undefined;
      node = next;
    }
    this.head = this.tail = undefined;
    return this;
  }
}
