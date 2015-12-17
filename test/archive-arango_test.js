/*global require, describe, it, before, after, beforeEach*/
/* jslint node: true, esnext: true */
"use strict";

/*
 * To execute these tests an arango database must be running
 */
// The URL used to execite the test
const url = "http://localhost:8529";

// The database name used for the tests
const dbname = "archive_arango_test";

const chai = require('chai');
chai.use(require("chai-as-promised"));
const should = chai.should();

const testConfiguration = {
	// Defines the names used for the collections
	"collections": {
		"collectionNameMetaData": "archiveMetaData",
		"collectionNameEdge": "archiveEdges",
		"collectionNameBlob": "archiveBlobData"
	},

	// defines the data base connection. See arango documentation for more details
	"dataBase": {
		"url": url,
		"databaseName": dbname
	}
};

// the db to create and remove the database
const db = require('arangojs')(url);

// the db to cleanup the collections
let db2;

let archive = require("../lib/archive-arango.js");
archive.initialize(testConfiguration);

describe("archive-arango", function () {
	before('Create the database for testing', function () {
		// create the database
		// console.log('BEFORE');
		return db.createDatabase(dbname).then(function (db) {
			return new Promise(function (fulfill, reject) {
				fulfill(db);
			});
		}, function (err) {
			if (err.errorNum == 1207) {
				// The database already exists
				return new Promise(function (fulfill, reject) {
					fulfill(true);
				});
			} else {
				return new Promise(function (fulfill, reject) {
					reject(err);
				});
			}
		});
	});

	after('Remove the test database', function () {
		// delete the collection
		return db.dropDatabase(dbname);
	});

	beforeEach('Cleanup the collections', function () {
		if (!db2) {
			db2 = require('arangojs')(testConfiguration.dataBase);
		}
		// clears all the collections
		return db2.truncate();
	});

	it("saves a blob and retrieves it again", function () {
		let msg = "My long blob message";

		return archive.save({
			"name": "test 1"
		}, msg).then(function (id) {
			return archive.get(id).should.eventually.equal(msg);
		});
	});

	it("saves a blob and deletes it again", function () {
		let msg = "My long blob message";

		return archive.save({
			"name": "test 1"
		}, msg).then(function (id) {

			return archive.delete(id).then(function (val) {
				// After all, all the collections should be empty
				return checkElementCount(0, 0, 0);

			});
		});
	});

	it("saves three blobs and deletes them again", function () {
		let msg1 = "My long blob message";
		let msg2 = "This is an other blob message";

		// save three blobs, two of them have the same message
		return Promise.all([
			archive.save({
				"name": "test 1"
			}, msg1),
			archive.save({
				"name": "test 2"
			}, msg2),
			archive.save({
				"name": "test 3"
			}, msg1)
		]).then(function (ids) {

			let metaId1 = ids[0];
			let metaId2 = ids[1];
			let metaId3 = ids[2];

			return checkElementCount(3, 3, 2).then(function () {

				// console.log("Delete the first element with the same blob as the third one");
				return archive.delete(metaId1).then(function () {
					return checkElementCount(2, 2, 2).then(function () {

						// console.log("Delete the second element");
						return archive.delete(metaId2).then(function () {
							return checkElementCount(1, 1, 1).then(function () {

								// console.log("Delete the third and last element");
								return archive.delete(metaId3).then(function () {
									return checkElementCount(0, 0, 0);
								});
							});
						});
					});
				});
			});
		});
	});



	it("Tes the list function", function () {

		let msg1 = "My long blob message";
		let msg2 = "This is an other blob message";

		// save three blobs, two of them have the same message
		return Promise.all([
			archive.save({
				"name": "test 1"
			}, msg1),
			archive.save({
				"name": "test 2"
			}, msg2),
			archive.save({
				"name": "test 3"
			}, msg1)
		]).then(function (ids) {
			let metaId1 = ids[0];
			let metaId2 = ids[1];
			let metaId3 = ids[2];

			// list without parameter should return three elements
			return archive.list().then(function (cursor) {
				return cursor.all().then(function (elements) {
					elements.length.should.equal(3);

					// now test with one parameter
					return archive.list("doc.name == 'test 3'").then(function (cursor) {
						return cursor.all().then(function (elements) {
							elements.length.should.equal(1);
						});
					});

				});
			});


		});

	});

});

/**
 * This function checks the count of elements for the three collections.
 * It returns a promise with true if all the ecpected counts are matching
 * or a a failed promise with the error if there is a difference
 *
 * @param metaCount The expected count for the meta data collection
 * @param edgeCount The expected count for the edge collection
 * @param blobCount The expected count for the blob data collection
 * @return promise
 */
function checkElementCount(metaCount, edgeCount, blobCount) {

	return Promise.all([
		db2.collection(testConfiguration.collections.collectionNameMetaData),
		db2.collection(testConfiguration.collections.collectionNameEdge),
		db2.collection(testConfiguration.collections.collectionNameBlob)
	]).then(function (collections) {
		// Now all the three collections are in the array
		return Promise.all([
			collections[0].all(),
			collections[1].all(),
			collections[2].all()
		]).then(function (arrayOfDocuments) {
			// now each array element contains a list of documents
			let error = '';
			if (arrayOfDocuments[0].length != metaCount) {
				error = error +
					`Expected '${metaCount}' elements in the meta collection but got '${arrayOfDocuments[0].length}' elements \n`;
			}
			if (arrayOfDocuments[1].length != edgeCount) {
				error = error +
					`Expected '${edgeCount}' elements in the edge collection but got '${arrayOfDocuments[1].length}' elements \n`;
			}
			if (arrayOfDocuments[2].length != blobCount) {
				error = error +
					`Expected '${blobCount}' elements in the blob collection but got '${arrayOfDocuments[2].length}' elements \n`;
			}

			if (error.length === 0) {
				return new Promise(function (fulfill, reject) {
					fulfill(true);
				});
			} else {
				return new Promise(function (fulfill, reject) {
					reject(error);
				});
			}

		});

	});



}
