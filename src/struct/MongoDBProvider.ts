import { MongoClient, InsertOneWriteOpResult, UpdateWriteOpResult, DeleteWriteOpResultObject } from "mongodb";
import { Provider, CachedProviderConstructor, GungnirError } from "@gungnir/core";

function connectMongo(uri: string): Promise<MongoClient> {
	return new Promise((resolve, reject) => {
		MongoClient.connect(uri, {
      useUnifiedTopology: true,
      useNewUrlParser: true
    }, (err, client) => {
			if (err) reject(err);
			else resolve(client);
		});
	});
}
function fetchMongo(client: MongoClient, table: string, find = {}): Promise<any> {
	let collection = client.db().collection(table);
	return new Promise((resolve, reject) => {
		collection.findOne(find, (err, res) => {
			if (err) reject(err);
			else resolve(res);
		});
	})
}
function insertMongo(client: MongoClient, table: string, data: {_id: string}): Promise<InsertOneWriteOpResult<{_id: string}>> {
	let collection = client.db().collection(table);
	return new Promise((resolve, reject) => {
		collection.insertOne(data, (err, res) => {
			if (err) reject(err);
			else resolve(res);
		});
	})
}
function updateMongo(client: MongoClient, table: string, find: {_id: string}, data: {[key: string]: Provider.ValueTypes | null}): Promise<UpdateWriteOpResult> {
	let collection = client.db().collection(table);
	return new Promise((resolve, reject) => {
		collection.updateOne(find, {$set: data}, (err, res) => {
			if (err) reject(err);
			else resolve(res);
		});
	})
}
function deleteMongo(client: MongoClient, table: string, find: {_id: string}): Promise<DeleteWriteOpResultObject> {
	let collection = client.db().collection(table);
	return new Promise((resolve, reject) => {
		collection.deleteOne(find, (err, res) => {
			if (err) reject(err);
			else resolve(res);
		});
	})
}

async function fetchData(client: MongoClient, table: string, id: string): Promise<{[key: string]: Provider.ValueTypes | null | undefined}> {
  let find = await fetchMongo(client, table, {_id: id});
  if (!find) return  {_id: id};
  else return find;
}
async function sendData(client: MongoClient, table: string, id: string, data: {[key: string]: Provider.ValueTypes | null}) {
	let find = await fetchMongo(client, table, {_id: id});
	let res = find ? await insertMongo(client, table, Object.assign({_id: id}, data)) : await updateMongo(client, table, {_id: id}, data);
	return res;
}
async function clearData(client: MongoClient, table: string, id: string) {
	let res = await deleteMongo(client, table, {_id: id});
	return res;
}

const CLIENT_NOT_READY = "the MongoDB client isn't connected, use 'MongoDBProvider.connect(uri: string)'.";

export class MongoDBProvider extends Provider {
	declare public static readonly cached: CachedProviderConstructor<typeof MongoDBProvider>;
	
	#mongoClient: MongoClient | null = null;
	public constructor(public readonly uri: string) {
		super();
		connectMongo(uri).then(client => {
			this.#mongoClient = client;
		});
	}

  public async get(table: string, id: string, key: string) {
    if (!this.#mongoClient) throw new GungnirError(CLIENT_NOT_READY);
    if (key == "_id") return id;
    const res = await fetchData(this.#mongoClient, table, id);
    return res[key] ?? null;
  }
  public async set(table: string, id: string, key: string, value: Provider.ValueTypes | null) {
    if (!this.#mongoClient) throw new GungnirError(CLIENT_NOT_READY);
    if (key == "_id") return;
    const res = await sendData(this.#mongoClient, table, id, {[key]: value});
    return res;
  }
  public async clear(table: string, id: string) {
    if (!this.#mongoClient) throw new GungnirError(CLIENT_NOT_READY);
    return clearData(this.#mongoClient, table, id);
  }
}