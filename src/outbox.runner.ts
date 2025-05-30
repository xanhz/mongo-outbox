import {
  ChangeStream,
  ChangeStreamInsertDocument,
  ChangeStreamOptions,
  Document,
  MongoClient,
  ResumeToken,
} from 'mongodb';
import { EventEmitter } from 'stream';
import { OutboxRunnerConfig } from './interfaces';
import * as _ from './utils/lodash';
import { TaskQueue } from './utils/task-queue';

export class OutboxRunner<T extends Document> {
  protected readonly client: MongoClient;
  protected readonly queue: TaskQueue;
  protected readonly ee: EventEmitter;
  protected stream!: ChangeStream<T, ChangeStreamInsertDocument<T>>;
  protected status: {
    client: 'connecting' | 'connected' | 'close';
    stream: 'connecting' | 'restarting' | 'running' | 'close';
  };

  constructor(protected readonly conf: OutboxRunnerConfig<T>) {
    this.queue = new TaskQueue();
    this.client = new MongoClient(conf.client.url, conf.client.options);
    this.ee = new EventEmitter();
    this.status = {
      client: 'connecting',
      stream: 'connecting',
    };
    this.client.on('close', () => {
      this.status.client = 'close';
      this.emit('close');
    });
    this.client.on('error', error => {
      this.emit('error', error);
    });
    this.client.on('connectionReady', () => {
      this.status.client = 'connected';
      this.emit('connected');
    });
  }

  public get health() {
    return {
      status: this.status,
    };
  }

  public emit(event: string, ...args: any[]) {
    return this.ee.emit(event, ...args);
  }

  public on(event: 'running', listener: (pipelines: any[], opts: ChangeStreamOptions) => void): this;
  public on(event: 'restarting', listener: () => void): this;
  public on(event: 'connected', listener: () => void): this;
  public on(event: 'close', listener: () => void): this;
  public on(event: 'error', listener: (err: any) => void): this;
  public on(event: 'committed', listener: (token: ResumeToken) => void): this;
  public on(event: 'change', listener: (change: ChangeStreamInsertDocument<T>) => void): this;
  public on(event: string, listener: (...args: any[]) => void) {
    this.ee.on(event, listener);
    return this;
  }

  public async start() {
    const resumeToken = await this.conf.storage.get();
    const pipeline = [
      {
        $match: {
          operationType: 'insert',
          ...this.conf.watch.filter,
        },
      },
    ];
    const options = { resumeAfter: resumeToken, ...this.conf.watch.options };

    this.stream = this.client.watch(pipeline, options);

    this.emit('running', pipeline, options);

    this.stream.on('change', change => {
      this.queue.push({
        execute: async () => {
          this.emit('change', change);
          await this.conf.publisher.publish(change);
          await this.conf.storage.set(change._id);
          this.emit('committed', change._id);
        },
        onerror: err => {
          this.emit('error', err);
          this.queue.clear();
          this.stream.close().catch(_.noop);
          this.status.stream = 'restarting';
          this.emit('restarting');
          return _.delay(1_000).then(() => this.start());
        },
      });
    });

    this.stream.once('error', err => {
      this.emit('error', err);
      this.stream.close().catch(_.noop);
      this.queue.clear();
      this.status.stream = 'restarting';
      this.emit('restarting');
      _.delay(1_000).then(() => this.start());
    });

    this.stream.once('end', () => {
      this.stream.close().catch(_.noop);
      this.queue.clear();
      this.status.stream = 'restarting';
      this.emit('restarting');
      _.delay(1_000).then(() => this.start());
    });

    this.stream.once('close', () => {
      this.status.stream = 'close';
    });

    this.status.stream = 'running';
  }

  public stop() {
    return this.client.close(true);
  }
}
