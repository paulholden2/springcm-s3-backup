const urlencode = require('urlencode');
const _ = require('lodash');
const moment = require('moment-timezone');
const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');
const MemoryStream = require('memorystream');
const s2b = require('stream-to-buffer');
const zoho = require('zoho-node-sdk');
const s3putDocument = require('./s3putDocument.js');

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
					'Date': moment.tz('America/Los_Angeles').format('MM/DD/YYYY hh:mm A')
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
	var docUploads = 0;
	var folderUploads = 0;
	const parallel = opts.parallel;
	const del = opts.delete;

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
				if (opts.verbose) {
					console.log(process.env.S3_BUCKET + ' found');
				}

				callback(null, s3);
			}
		},
		(s3, callback) => {
			if (opts.verbose) {
				console.log('Getting root folder');
			}

			SpringCM.folder.root((err, root) => {
				if (err) {
					return callback(err);
				}

				callback(null, s3, root);
			});
		},
		(s3, root, callback) => {
			var q = async.queue((folder, callback) => {
				var uploadFolder = false;
				const fuid = folder.href.self.slice(-36);

				async.waterfall([
					(callback) => {
						callback();
					},
					(callback) => {
						// Queue up subfolders
						SpringCM.folder.folders(folder, (err, folders) => {
							if (err) {
								return callback(err);
							}

							folders.filter(f => f.name !== 'Trash').forEach(f => q.push(f));
							callback(null, folder);
						});
					},
					(folder, callback) => {
						// Request folder to get attributes
						SpringCM.folder.uid(folder.href.self.slice(-36), (err, fld) => {
							if (err) {
								return callback(err);
							}

							callback(null, fld);
						});
					},
					(folder, callback) => {
						// Request document to get attributes
						SpringCM.folder.documents(folder, (err, documents) => {
							if (err) {
								return callback(err);
							}

							async.mapSeries(documents, (doc, callback) => {
								SpringCM.document.uid(doc.href.self.slice(-36), (err, doc) => {
									if (err) {
										return callback(err);
									}

									callback(null, doc);
								});
							}, (err, documents) => {
								if (err) {
									return callback(err);
								}

								callback(null, folder, documents);
							});
						});
					},
					(folder, documents, callback) => {
						async.eachSeries(documents, (doc, callback) => {
							const docid = doc.href.self.slice(-36);
							const key = 'document/' + docid;

							async.waterfall([
								(callback) => {
									s3.headObject({
										Bucket: process.env.S3_BUCKET,
										Key: key
									}, (err, data) => {
										if (err) {
											// If error because object doesn't exist
											if (err.code === 'NotFound') {
												return callback(null, false);
											} else {
												return callback(err);
											}
										}

										var lastBackup = moment(data.LastModified);
										var updated = moment(doc.updated);

										callback(null, lastBackup.isAfter(updated));
									});
								},
								(recent, callback) => {
									if (recent && !opts.overwrite) {
										return callback(null, null);
									}

									uploadFolder = true;

									var stream;

									SpringCM.document.download(doc, () => {
										stream = new MemoryStream();
										return stream;
									}, (err) => {
										if (err) {
											return callback(err);
										}

										docUploads += 1;

										callback(null, stream);
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
										s3putDocument(s3, doc, key, buffer, (err, data) => {
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
									callback();
								}
							], (err) => {
								callback(err);
							});
						}, (err) => {
							if (err) {
								return callback(err);
							}

							callback(null, folder);
						});
					},
					(folder, callback) => {
						s3.headObject({
							Bucket: process.env.S3_BUCKET,
							Key: 'folder/' + folder.href.self.slice(-36)
						}, (err, data) => {
							if (err) {
								// If error because object doesn't exist
								if (err.code === 'NotFound') {
									uploadFolder = true;
									return callback(null, folder);
								} else {
									return callback(err);
								}
							}

							var lastBackup = moment(data.LastModified);
							var updated = moment(folder.updated);

							if (!lastBackup.isAfter(updated)) {
								uploadFolder = true;
							}

							callback(null, folder);
						});
					},
					(folder, callback) => {
						if (!uploadFolder) {
							console.log(`folder/${folder.href.self.slice(-36)} up-to-date; nothing changed`);
							return callback(null, folder);
						}

						// Upload folder
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

							folderUploads += 1;

							callback(null, folder);
						});
					},
					(folder, callback) => {
						if (!uploadFolder) {
							return callback();
						}

						// Upload folder attributes
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

							callback(null, folder);
						});
					}
				], (err) => {
					callback(err);
				});
			}, parallel);

			q.push(root);

			q.drain = () => {
				callback();
			};
		},
		(callback) => {
			if (opts.verbose) {
				console.log(`${docUploads} documents uploaded`);
				console.log(`${folderUploads} folders uploaded`);
			}

			callback();
		},
		(callback) => {
			if (opts['no-log']) {
				return callback();
			}

			log_backup(opts, callback);
		}
	], (err) => {
		if (err) {
			console.log(err);
		}
	});
}

module.exports = backup;
