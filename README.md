# Mongo Outbox Pattern

This repository implements mongodb [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for reliable publishing event in microservices.

# Getting started

```ts
import { OutboxRunner } from '@xanhz/mongo-outbox';
import dotenv from 'dotenv';
import fs from 'fs';
import { Document, MongoClient, ObjectId, ResumeToken } from 'mongodb';

dotenv.config({});

const MONGODB_URI = process.env.MONGODB_URI!;
const TOKEN_FILEPATH = process.env.TOKEN_FILEPATH!;

const logger = {
  print(level: string, msg: any, ...args: any[]) {
    if (msg instanceof Error) {
      console.log(`[${new Date().toISOString()}] - [${level.toUpperCase()}] - [${this.name}]:`, msg, ...args);
    } else {
      console.log(`[${new Date().toISOString()}] - [${level.toUpperCase()}] - [${this.name}]: ${msg}`, ...args);
    }
  }
  info(msg: string, ...args: any[]) {
    this.print('info', msg, ...args);
  }
  error(err: any, ...args: any[]) {
    this.print('error', err, ...args);
  }
}

interface OutboxDocument<TPayload = any> extends Document {
  _id: ObjectId;
  event: string;
  payload: TPayload;
  timestamp: Date;
}

async function main() {
  const runner = new OutboxRunner<OutboxDocument>({
    client: {
      url: MONGODB_URI,
    },
    watch: {
      filter: {
        'ns.coll': 'outbox', // listen all collections named 'outbox' in cluster
      },
    },
    storage: {
      get() {
        return new Promise<ResumeToken>((resolve, reject) => {
          fs.readFile(TOKEN_FILEPATH, (err, raw) => {
            if (err) {
              return resolve(undefined);
            }
            const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : raw;
            return resolve(JSON.parse(str));
          });
        });
      },
      set(token) {
        return new Promise<void>((resolve, reject) => {
          const str = JSON.stringify(token, undefined, 0);
          fs.writeFile(TOKEN_FILEPATH, str, { encoding: 'utf8' }, err => {
            return err ? reject(err) : resolve(void 0);
          });
        });
      },
    },
    publisher: {
      publish(doc) {
        return Promise.resolve();
      },
    },
  });

  runner.on('error', err => logger.error(err));
  runner.on('close', () => logger.info('MongoDB is closed'));
  runner.on('connected', () => logger.info('MongoDB is connected'));
  runner.on('running', (pipeline, opts) => logger.info('Starting stream pipelines=%j | options=%j', pipeline, opts));
  runner.on('restarting', () => logger.info('Change stream is restarting'));
  runner.on('change', change => logger.info('Change=%j', change));
  runner.on('committed', token => logger.info('Committed token=%j', token));

  runner.start();

  const client = new MongoClient(MONGODB_URI);

  await client
    .db('test')
    .collection('outbox')
    .insertOne({
      event: 'orders.created',
      payload: {
        code: 'RX-123456',
      },
      timestamp: new Date(),
    });
}

main();
```
