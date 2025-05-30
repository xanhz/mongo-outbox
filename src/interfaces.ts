import {
  ChangeStreamInsertDocument,
  ChangeStreamOptions,
  Document,
  MongoClientOptions,
  ResumeToken,
  RootFilterOperators,
} from 'mongodb';

export interface IResumeTokenStorage {
  get(): Promise<ResumeToken>;
  set(token: ResumeToken): Promise<void>;
}

export interface IOutboxPublisher<T extends Document> {
  publish(doc: ChangeStreamInsertDocument<T>): Promise<void>;
}

export interface OutboxRunnerConfig<T extends Document> {
  client: {
    url: string;
    options?: MongoClientOptions;
  };
  watch: {
    filter: RootFilterOperators<ChangeStreamInsertDocument<T>>;
    options?: ChangeStreamOptions;
  };
  storage: IResumeTokenStorage;
  publisher: IOutboxPublisher<T>;
}
