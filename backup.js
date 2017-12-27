const urlencode = require('urlencode');
const _ = require('lodash');
const moment = require('moment-timezone');
const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');
const MemoryStream = require('memorystream');
const s2b = require('stream-to-buffer');
const zoho = require('zoho-node-sdk');

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

function log_backup(opts, callback) {
	async.waterfall([
		(callback) => {
			if (opts.verbose) {
				console.log('Getting SpringCM account details');
			}

			SpringCM.account((err, account) => {
				if (err) {
					return callback(err);
				}

				callback(null, account);
			});
		},
		(account, callback) => {
			if (!account) {
				return callback();
			}

			zoho.reports(process.env.REPORTS_EMAIL_ID, process.env.REPORTS_AUTHTOKEN, (err, reports) => {
				if (err) {
					return callback(err);
				}

				callback(null, reports, account);
			});
		},
		(reports, account, callback) => {
			if (!reports) {
				return callback();
			}

			if (opts.verbose) {
				console.log(`Logging successful backup for ${account.name} (${account.id})`);
			}

			reports.table('Client Reporting Database', 'SpringCM Backup History', (err, t) => {
				if (err) {
					return callback(err);
				}

				t.addrow({
					'Account Name': account.name,
					'Account ID': account.id,
					'Date': moment.tz('America/Los_Angeles').format('MM/DD/YYYY HH:mm A')
				}, (err, row) => {
					if (err) {
						return callback(err);
					}

					callback();
				});
			});
		}
	], (err) => {
		callback(err);
	});
}

function backup(opts) {
	async.waterfall([
		// SpringCM auth
		(callback) => {
			if (opts.verbose) {
				console.log('Authenticating with SpringCM API user');
			}

			SpringCM.auth.login(process.env.SPRINGCM_DATACENTER, process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
				if (err) {
					return callback(err);
				}

				callback();
			});
		},
		(callback) => {
			callback(null, new AWS.S3());
		},
		(s3, callback) => {
			if (opts.verbose) {
				console.log('Locating bucket: ' + process.env.S3_BUCKET);
			}

			s3.listBuckets({}, (err, data) => {
				if (err) {
					return callback(err);
				}

				if (data.Buckets.map(bucket => bucket.Name).indexOf(process.env.S3_BUCKET) < 0) {
					return callback(null, false, s3);
				}

				callback(null, true, s3);
			});
		},
		(exists, s3, callback) => {
			if (!exists) {
				if (opts.verbose) {
					console.log('Not found; creating new bucket: ' + process.env.S3_BUCKET);
				}

				s3.createBucket({
					Bucket: process.env.S3_BUCKET
				}, (err, data) => {
					if (err) {
						return callback(err);
					}

					callback(null, s3);
				});
			} else {
				callback(null, s3);
			}
		},
		(s3, callback) => {
			SpringCM.folder.root((err, root) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, root);
			});
		},
		(s3, root, callback) => {
			if (opts.verbose) {
				console.log('Building SpringCM folder list');
			}

			subfolders(root, (err, folders) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders);
			});
		},
		(s3, folders, callback) => {
			if (opts.verbose) {
				console.log('Building SpringCM document list');
			}

			async.mapLimit(folders, 15, (folder, callback) => {
				SpringCM.folder.documents(folder, (err, documents) => {
					if (err) {
						return callback(err);
					}

					async.mapSeries(documents, (d, callback) => {
						if (opts.verbose) {
							console.log('Getting attributes for document/' + d.href.self.slice(-36));
						}

						SpringCM.document.uid(d.href.self.slice(-36), (err, doc) => {
							if (err) {
								return callback(err);
							}

							callback(null, doc);
						});
					}, (err, documents) => {
						if (err) {
							return callback(err);
						}

						callback(null, documents);
					});
				});
			}, (err, results) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, folders, [].concat.apply([], results));
			});
		},
		(s3, folders, documents, callback) => {
			async.mapLimit(folders, 15, (f, callback) => {
				const fuid = f.href.self.slice(-36);

				if (opts.verbose) {
					console.log('Getting attributes for folder/' + fuid);
				}

				SpringCM.folder.uid(fuid, (err, folder) => {
					if (err) {
						return callback(err);
					}

					callback(null, folder);
				});
			}, (err, result) => {
				callback(null, s3, result, documents);
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
					Bucket: process.env.S3_BUCKET,
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
					Bucket: process.env.S3_BUCKET,
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
			async.eachLimit(folders, 15, (folder, callback) => {
				const fuid = folder.href.self.slice(-36);

				async.waterfall([
					(callback) => {
						if (opts.verbose) {
							console.log(`folder/${fuid} uploaded`);
						}

						s3.putObject({
							Bucket: process.env.S3_BUCKET,
							Key: 'folder/' + fuid,
							Metadata: {
								'filepath': folder.path
							}
						}, (err, data) => {
							if (err) {
								return callback(err);
							}

							callback();
						});
					},
					(callback) => {
						if (opts.verbose) {
							console.log(`attributes/${fuid} uploaded`);
						}

						s3.putObject({
							Body: JSON.stringify(folder.attributes),
							Bucket: process.env.S3_BUCKET,
							Key: 'attributes/' + fuid,
							Metadata: {
								'filepath': folder.path
							}
						}, (err, data) => {
							if (err) {
								return callback(err);
							}

							callback();
						});
					},
					(callback) => {
						delete depot.folders['folder/' + fuid];
						callback();
					}
				], (err) => {
					if (err) {
						return callback(err);
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
								console.log(`document/${docid} uploaded`);
							}

							s3.putObject({
								Body: buffer,
								Bucket: process.env.S3_BUCKET,
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
								Bucket: process.env.S3_BUCKET,
								Key: key
							}, (err, data) => {
								if (err) {
									return callback(err);
								}

								if (data.Metadata.filename !== doc.name || data.Metadata.filepath !== doc.path) {
									if (opts.verbose) {
										console.log(`document/${docid} up-to-date; updating metadata`);
									}

									s3.copyObject({
										Bucket: process.env.S3_BUCKET,
										Key: key,
										CopySource: urlencode(`${process.env.S3_BUCKET}/${key}`),
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

									callback();
								}
							});
						}
					},
					(callback) => {
						if (opts.verbose) {
							console.log(`attributes/${docid} uploaded`);
						}

						s3.putObject({
							Body: JSON.stringify(doc.attributes),
							Bucket: process.env.S3_BUCKET,
							Key: 'attributes/' + docid
						}, (err) => {
							if (err) {
								return callback(err);
							}

							callback();
						});
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
					Bucket: process.env.S3_BUCKET,
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
		},
		(callback) => {
			if (opts['no-log']) {
				return callback();
			}

			log_backup(opts, callback);
		}
	], (err) => {
		if (err) {
			return console.log(err);
		}
	});
}

module.exports = backup;
