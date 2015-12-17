/* jslint node: true, esnext: true */

"use strict";

const log4js = require('log4js');
const logger = log4js.getLogger('hyperion_archive_arango');
logger.setLevel(log4js.levels.ERROR);

const md5 = require('md5');
const _ = require('lodash');

// The arango db instance, will be set by initialize
let db;

// Stores the current configuration
let config;

// The collection cache stores all the needed collection
let collectionCache = {};

// defines the names used in the collection cache
const metaName = "meta";
const edgeName = "edge";
const blobName = "blob";

const defaultConfiguration = {
	// Defines the names used for the collections
	"collections": {
		"collectionNameMetaData": "archiveMetaData",
		"collectionNameEdge": "archiveEdges",
		"collectionNameBlob": "archiveBlobData"
	},

	// defines the data base connection. See arango documentation for more details
	"dataBase": {
		"url": "http://localhost:8000",
		"databaseName": "archive"
	}
};

module.exports = {
	/**
	 * Initialize the arango database and set the collection names used.
	 * @param config The configuration hash
	 * @return nothing.
	 */
	"initialize": function (conf) {
		if (!conf) {
			// no configuration given at all
			conf = defaultConfiguration;
			logger.info("No configuration given, use default configuration");
		} else {
			// Merge missing configuration elements from the default configuration
			_.defaultsDeep(config, defaultConfiguration);
		}

		logger.info("Uses the configuration:\n" + JSON.stringify(conf));

		// Stores the new configuration
		config = conf;

		db = require('arangojs')(config.dataBase);
	},

	/**
	 * Save a complete attachement with its meta data.
	 * The first promise saves the meta data and the blob data independently.
	 * Then the next promise saves the edge which combines both documents.
	 * @param meta The meta data hash
	 * @param blob The blob content
	 * @return promise A promise with the id of the generated meta object
	 */
	"save": function (meta, blob) {
		let timeStamp = Date.now();
		if (meta._time_stamp === undefined) {
			meta._time_stamp = timeStamp;
		} else {
			timeStamp = meta._time_stamp;
		}

		// Check that there is data given
		if (blob === undefined) {
			return Promise.reject("No blob data given to be saved");
		}

		// Check that there is data given
		if (meta === undefined) {
			return Promise.reject("No meta data given to be saved");
		}

		return _initCollectionCache(config.collections).then(function () {
			return Promise.all([_saveMeta(meta), _saveBlob(blob, timeStamp)]).then(function (resArray) {
				let myMetaId = resArray[0];
				return _saveEdge(resArray[0], resArray[1], timeStamp).then(function (val) {
					return new Promise(function (fulfill, reject) {
						fulfill(myMetaId);
					});
				});
			});
		});
	},

	/**
	 * List the existing meta data objects. If a filter is given it will only
	 * return the objects matching the filter. See the arango AQL FILTER description for
	 * more details.
	 * @param filter The FILTER used to reduce the result. See the AQL description for more information
	 * @param sort The SORT used to sort the result.
	 * @return promise A promise with the result cursor.
	 */
	"list": function (filter, sort) {

		let queryString = 'FOR doc IN ' + config.collections.collectionNameMetaData;
		if (filter !== undefined) {
			queryString = queryString + ' FILTER ' + filter;
		}

		if (sort !== undefined) {
			queryString = queryString + ' SORT ' + filter;
		}

		queryString = queryString + ' RETURN doc';

		logger.debug(`The query is:\n ${queryString}`);

		return _initCollectionCache(config.collections).then(function () {
			return db.query(queryString);
		});
	},

	/**
	 * Returns the blob data for the given id.
	 * @param id The id of the metaData object
	 * @return promise A promise with the blob content
	 */
	"get": function (id) {
		logger.debug(`GET : '${id}'`);
		if (id === undefined) {
			return Promise.reject("No id given to retrieve the data for");
		}

		return _initCollectionCache(config.collections).then(function () {
			return _getBlobData(id);
		});
	},

	/**
	 * Removes the archived element for the given meta data id.
	 * The blob data will only be removed if there is no meta data object
	 * referencing this blob any more.
	 * @param id The id of the metaData object
	 * @return promise A promise with the status of the result of this operation
	 */
	"delete": function (id) {
		logger.debug(`DELETE : '${id}'`);

		if (id === undefined) {
			return Promise.reject("No id given to remove the data for");
		}

		return _initCollectionCache(config.collections).then(function () {
			return _remove(id);
		});
	}

};


/**
 * Remove the archive entry. First find all the docs, then proof if other edges exists
 * to the blob, if not remove also the blob.
 * @param id The id of the meta data document
 * @return promise A promise with the result of the remove action
 */
function _remove(id) {
	return collectionCache[edgeName].outEdges(id).then(function (arrayEdgeDoc) {

		if (arrayEdgeDoc === undefined) {
			return new Promise(function (fulfill, reject) {
				logger.debug(`The id '${id}' does not exists, nothing to remove`);
				fulfill(`The id '${id}' does not exists, nothing to remove`);
			});
		} else {
			// we got the edge, now we need to retrieve the blob doc
			return collectionCache[blobName].document(arrayEdgeDoc[0]._to).then(function (blobDoc) {
				// we got the blob doc check if there are other edges referenceing this blob
				return collectionCache[edgeName].inEdges(blobDoc._id).then(function (arrayEdgeDoc2) {
					if (arrayEdgeDoc2.length == 1) {
						logger.debug(`Blob with id '${blobDoc._id}' not used by other entries, delete it`);

						// only one edge is using this blob, remove it.
						return Promise.all([
							collectionCache[metaName].remove(id),
							collectionCache[edgeName].remove(arrayEdgeDoc[0]._id),
							collectionCache[blobName].remove(blobDoc._id)
						]);

					} else {
						// the blob should stay
						logger.debug(`Blob with id '${blobDoc._id}' is used by other entries. Do not delete blob`);

						return Promise.all([
							collectionCache[metaName].remove(id),
							collectionCache[edgeName].remove(arrayEdgeDoc[0]._id)
						]);
					}
				});
			});
		}
	});
}


/**
 * Returns a promise which has the blob data as the result.
 * @param id The id of the meta data document
 * @return promise A promise with the blob data
 */
function _getBlobData(id) {
	return collectionCache[edgeName].outEdges(id).then(function (edgeDoc) {
		// we got the edge, now we need to retrieve the blob doc
		return collectionCache[blobName].document(edgeDoc[0]._to).then(function (blobDoc) {
			// we got the blob doc
			return Promise.resolve(new Buffer(blobDoc.data, 'base64').toString('ascii'));
		});
	});
}


/**
 * Saves a blob document to the data store. Special for this objects is that a blob
 * will only be saved once. So if the same data will be inserted multiple times,
 * the blob itself will be stored only once.
 * @param blob The blob data document to be saved
 * @param timeStamp The timeStamp to use for this document
 * @return promise A promise with the id of the saved document
 */
function _saveBlob(blob, timeStamp) {

	// first get the md5 for this blob
	const blobHash = md5(blob);

	logger.debug(`_saveBlob: hash = '${blobHash}'`);

	return db.query(
		// The query returns a cursor with an already existing blob with the same hash
		'FOR doc IN ' + config.collections.collectionNameBlob + ' FILTER doc._key == "' + blobHash +
		'"  RETURN doc._id').then(function (cursor) {

		if (cursor.hasNext()) {
			// yes there is a value, this means the blob exists in the db

			logger.debug(`Blob already exists`);

			// returns a promise with the result from the cursor
			return cursor.next();
		} else {
			// the blob does not exists, create it

			// the key of the object is the blob hash,
			// The data is the blob as base64
			return collectionCache[blobName].save({
				"_key": blobHash,
				"data": new Buffer(blob).toString('base64'),
				"_time_stamp": timeStamp
			}).then(function (doc) {
				logger.debug(`Created a new blob data with the id '${doc._id}'`);
				return Promise.resolve(doc._id);
			}, function (err) {
				if (err.errorNum == 1210) {
					// This error may happen if the same blob will be saved at the same time.
					// In this case we can just ignore the error as it means the blob exits.
					// So we could return a fullfilled Promise
					return Promise.resolve(config.collections.collectionNameBlob + "/" + blobHash);
				} else {
					return Promise.reject(err);
				}

			});
		}
	});
}

/**
 * Saves a meta document to the data store.
 * @param doc The meta data document to be saved
 * @return promise A promise with the key of the saved document
 */
function _saveMeta(doc) {
	return collectionCache[metaName].save(doc).then(function (doc) {
		return Promise.resolve(doc._id);
	});
}

/**
 * Saves a edge document which connects to the meta data with the
 * existing blob.
 * @param metaKey The key of the meta data document
 * @param blobKey The key of the blob data document
 * @param timeStamp The timeStamp to use for this document
 * @return promise A promise with the key of the saved edge
 */
function _saveEdge(metaKey, blobKey, timeStamp) {
	return collectionCache[edgeName].save({
		"_time_stamp": timeStamp
	}, metaKey, blobKey).then(function (doc) {
		return Promise.resolve(doc._key);
	});
}



/**
 * Ensures that the collection chache contains all the collections needed
 *
 * Example config:
 *  const dataConfig = {
 *  	"collectionNameMetaData": "archiveMetaData",
 *  	"collectionNameEdge": "archiveEdges",
 * 	  "collectionNameBlob": "archiveBlobData"
 *  };
 *
 * @param config An object containing the collection names.
 * @return a Promise when the cache is filled
 */
function _initCollectionCache(config) {
	return Promise.all([
		_getCollection(config.collectionNameMetaData, collectionCache, metaName),
		_getCollection(config.collectionNameBlob, collectionCache, blobName),
		_getEdgeCollection(config.collectionNameEdge, collectionCache, edgeName)
	]);
}


/**
 * Returns a promise which will have the retrieved or created collection.
 * Once a collection is retrieved it will be cached in the given collectionCache.
 *
 * @param collectionName The name of the collection
 * @param collectionCache The cache which stores a retrieved collection
 * @param cacheName The name used to store the collection in the hash
 *
 * @return promise(collection) The collection used to save or load the data from as a promise
 */
function _getCollection(collectionName, collectionCache, cacheName) {
	if (collectionCache[cacheName]) {
		// the collection is already in the cache
		return Promise.resolve(collectionCache[cacheName]);
	} else {
		// try to retrive the existing collection
		return db.collection(collectionName).then(function (col) {
			collectionCache[cacheName] = col;
			return Promise.resolve(collectionCache[cacheName]);
		}, function (err) {
			if (err.errorNum == 1203) {
				// The collection does not exists in the database , create it
				// The collection does not exists

				logger.info(`Create new collection '${collectionName}'`);

				return db.createCollection(collectionName).then(function (col) {
					collectionCache[cacheName] = col;
					return Promise.resolve(collectionCache[cacheName]);
				});
			} else {
				// Any other error
				return Promise.reject(err);
			}
		});
	}
}

/**
 * Returns a promise which will have the retrieved or created edgeCollection.
 * Once a collection is retrieved it will be cached in the given collectionCache.
 *
 * @param collectionName The name of the collection
 * @param collectionCache The cache which stores a retrieved collection
 * @param cacheName The name used to store the collection in the hash
 *
 * @return promise(collection) The collection used to save or load the data from as a promise
 */
function _getEdgeCollection(collectionName, collectionCache, cacheName) {
	if (collectionCache[collectionName]) {
		// the collection is already in the cache
		return Promise.resolve(collectionCache[cacheName]);
	} else {
		// try to retrive the existing collection
		return db.edgeCollection(collectionName).then(function (col) {
			collectionCache[cacheName] = col;
			return Promise.resolve(collectionCache[cacheName]);
		}, function (err) {
			if (err.errorNum == 1203) {
				// The collection does not exists in the database , create it
				// The collection does not exists

				logger.info(`Create new edge collection '${collectionName}'`);

				return db.createEdgeCollection(collectionName).then(function (col) {
					collectionCache[cacheName] = col;
					return Promise.resolve(collectionCache[cacheName]);
				});
			} else {
				// Any other error
				return Promise.reject(err);
			}
		});
	}
}
