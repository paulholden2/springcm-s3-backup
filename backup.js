const urlencode = require('urlencode');
const _ = require('lodash');
const moment = require('moment');
const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');
const MemoryStream = require('memorystream');
const s2b = require('stream-to-buffer');

function backup(opts) {
	// Get all subfolders recursively, except for /Trash/ and its subfolders
	function subfolders(root, callback) {
		var folderlist = [ root ];
		var q = async.queue((folder, callback) => {
			SpringCM.folder.folders(folder, (err, folders) => {
				if (err) {
					return callback(err);
				}

				folders = folders.filter(folder => folder.path !== '/Trash/');
				folderlist = folderlist.concat(folders);
				folders.forEach(folder => q.push(folder));
				callback();
			});
		}, 15);

		q.drain = () => {
			callback(null, folderlist);
		};

		q.push(root);
	}

	async.waterfall([
		// SpringCM auth
		(callback) => {
			if (opts.verbose) {
				console.log('Authenticating with SpringCM API user');
			}

			SpringCM.auth.login(opts['data-center'], opts.id, opts.secret, (err, token) => {
				if (err) {
					return callback(err);
				}

				callback();
			});
		},
		(callback) => {
			SpringCM.folder.root((err, root) => {
				if (err) {
					return callback(err);
				}

				callback(null, root);
			});
		},
		(root, callback) => {
			if (opts.verbose) {
				console.log('Building SpringCM folder list');
			}

			subfolders(root, (err, folders) => {
				if (err) {
					return callback(err);
				}

				callback(null, folders);
			});
		},
		(folders, callback) => {
			if (opts.verbose) {
				console.log('Building SpringCM document list');
			}

			async.mapLimit(folders, 15, (folder, callback) => {
				SpringCM.folder.documents(folder, (err, documents) => {
					if (err) {
						return callback(err);
					}

					callback(null, documents);
				});
			}, (err, results) => {
				if (err) {
					return callback(err);
				}

				callback(null, folders, [].concat.apply([], results));
			})
		},
		(folders, documents, callback) => {
			callback(null, folders, documents);
		},
		(folders, documents, callback) => {
			var s3 = new AWS.S3();

			callback(null, s3, folders, documents);
		},
		(s3, folders, documents, callback) => {
			if (opts.verbose) {
				console.log('Locating bucket: ' + opts.bucket);
			}

			s3.listBuckets({}, (err, data) => {
				if (err) {
					return callback(err);
				}

				if (data.Buckets.map(bucket => bucket.Name).indexOf(opts.bucket) < 0) {
					return callback(new Error('No such bucket: ' + opts.bucket));
				}

				callback(null, s3, folders, documents);
			});
		},
		(s3, folders, documents, callback) => {
			var count = 1;
			var marker = null;
			var depot = {
				documents: [],
				folders: []
			};

			async.until(() => {
				return count === 0;
			}, (callback) => {
				var params = {
					Bucket: opts.bucket,
					Prefix: 'document/',
					MaxKeys: 1000
				};

				if (marker) {
					params.Marker = marker;
					marker = null;
				}

				s3.listObjects(params, (err, data) => {
					if (err) {
						return callback(err);
					}

					count = data.Contents.length;

					if (count > 0) {
						depot.documents = depot.documents.concat(data.Contents);
						marker = data.Contents[count - 1].Key;
					}

					callback();
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders, documents, depot);
			});
		},
		(s3, folders, documents, depot, callback) => {
			var count = 1;
			var marker = null;

			async.until(() => {
				return count === 0;
			}, (callback) => {
				var params = {
					Bucket: opts.bucket,
					Prefix: 'folder/',
					MaxKeys: 1000
				};

				if (marker) {
					params.Marker = marker;
					marker = null;
				}

				s3.listObjects(params, (err, data) => {
					if (err) {
						return callback(err);
					}

					count = data.Contents.length;

					if (count > 0) {
						depot.folders = depot.folders.concat(data.Contents);
						marker = data.Contents[count - 1].Key;
					}

					callback();
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders, documents, depot);
			});
		},
		(s3, folders, documents, depot, callback) => {
			if (opts.verbose) {
				console.log(`${folders.length} folders in SpringCM`);
				console.log(`${documents.length} documents in SpringCM`);
				console.log(`${depot.folders.length} folders backed up in S3`);
				console.log(`${depot.documents.length} documents backed up in S3`);
			}

			depot.documents = _.zipObject(depot.documents.map(d => d.Key), depot.documents);
			depot.folders = _.zipObject(depot.folders.map(f => f.Key), depot.folders);

			callback(null, s3, folders, documents, depot);
		},
		(s3, folders, documents, depot, callback) => {
			if (opts.verbose) {
				console.log('Backing up folders');
			}

			async.eachLimit(folders, 15, (folder, callback) => {
				const key = 'folder/' + folder.href.self.slice(-36);

				s3.putObject({
					Bucket: opts.bucket,
					Key: key,
					Metadata: {
						'filepath': folder.path
					}
				}, (err, data) => {
					if (err) {
						return callback(err);
					}

					delete depot.folders[key];

					callback();
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders, documents, depot);
			});
		},
		(s3, folders, documents, depot, callback) => {
			async.eachLimit(documents, 15, (doc, callback) => {
				var memstream = new MemoryStream();
				const docid = doc.href.self.slice(-36);
				const key = 'document/' + docid;

				async.waterfall([
					(callback) => {
						if (depot.documents.hasOwnProperty(key)) {
							var lastBackup = moment(depot.documents[key].LastModified);
							var updated = moment(doc.updated);

							// If the last backup date of the doc is after the
							// update date on the doc in SpringCM, no need to back up
							if (lastBackup.isAfter(updated)) {
								return callback(null, true);
							}
						}

						// Default to making a backup of the doc
						return callback(null, false);
					},
					(recent, callback) => {
						if (recent) {
							return callback(null, null);
						}

						SpringCM.document.download(doc, memstream, (err) => {
							if (err) {
								return callback(err);
							}

							callback(null, memstream);
						});
					},
					(stream, callback) => {
						if (!stream) {
							return callback(null, null);
						}

						s2b(stream, (err, buffer) => {
							if (err) {
								return callback(err);
							}

							callback(null, buffer);
						});
					},
					(buffer, callback) => {
						if (buffer) {
							if (opts.verbose) {
								console.log(`Backing up ${doc.path} to document/${docid}`);
							}

							s3.putObject({
								Body: buffer,
								Bucket: opts.bucket,
								Key: key,
								Metadata: {
									filename: doc.name,
									filepath: doc.path
								}
							}, (err, data) => {
								if (err) {
									return callback(err);
								}

								callback();
							});
						} else {
							s3.headObject({
								Bucket: opts.bucket,
								Key: key
							}, (err, data) => {
								if (err) {
									return callback(err);
								}

								if (data.Metadata.filename !== doc.name || data.Metadata.filepath !== doc.path) {
									if (opts.verbose) {
										console.log(`document/${docid} up-to-date; updating tags`);
									}

									s3.copyObject({
										Bucket: opts.bucket,
										Key: key,
										CopySource: urlencode(`${opts.bucket}/${key}`),
										Metadata: {
											filename: doc.name,
											filepath: doc.path
										}
									}, (err, data) => {
										if (err) {
											return callback(err);
										}

										callback();
									});
								} else {
									if (opts.verbose) {
										console.log(`document/${docid} up-to-date; nothing changed`);
									}
								}

								callback();
							});
						}
					},
					(callback) => {
						delete depot.documents[key];

						callback();
					}
				], (err) => {
					callback(err);
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders, documents, depot);
			});
		},
		(s3, folders, documents, depot, callback) => {
			var keys = [];

			keys = keys.concat(Object.keys(depot.documents), Object.keys(depot.folders));

			async.eachLimit(keys, 15, (obj, callback) => {
				s3.deleteObject({
					Bucket: opts.bucket,
					Key: obj
				}, (err, data) => {
					if (err) {
						return callback(err);
					}

					if (opts.verbose) {
						console.log(`${obj} not found; removed from backup`);
					}

					callback();
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback();
			});
		}
	], (err) => {
		if (err) {
			return console.log(err);
		}
	});
}

module.exports = backup;