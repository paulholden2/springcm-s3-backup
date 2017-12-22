const dotenv = require('dotenv');
const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');
const MemoryStream = require('memorystream');
const s2b = require('stream-to-buffer');
const clcmd = require('command-line-commands');
const clarg = require('command-line-args');
const clusg = require('command-line-usage');

const commands = [
	null,
	'backup'
];

const options = [
	{
		name: 'verbose',
		alias: 'v',
		description: 'Enables verbose logging of events and operations.',
		type: Boolean
	},
	{
		name: 'id',
		alias: 'i',
		description: 'The client ID for the SpringCM API user to authenticate with.',
		type: String
	},
	{
		name: 'secret',
		alias: 's',
		description: 'The client secret for the SpringCM API user to authenticate with.',
		type: String
	},
	{
		name: 'bucket',
		alias: 'b',
		description: 'The name of the AWS S3 bucket.',
		type: String
	},
	{
		name: 'help',
		alias: 'h',
		description: 'Displays this usage dialog.',
		type: Boolean
	}
];

const { cmd, argv } = clcmd(commands);
const opts = clarg(options, {
	argv: argv
});

const sections = [
	{
		header: 'SpringCM AWS S3 Backup',
		content: 'Performs iterative backups of a SpringCM account to AWS S3'
	},
	{
		header: 'Options',
		optionList: options
	}
];

if (!cmd && opts.help) {
	return console.log(clusg(sections));
}

dotenv.config();

// Get all subfolders recursively, except for /Trash/ and its subfolders
function subfolders(root, callback) {
	var folderlist = [];
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
		SpringCM.auth.login(process.env.SPRINGCM_DATACENTER, process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
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
		subfolders(root, (err, folders) => {
			if (err) {
				return callback(err);
			}

			callback(null, folders);
		});
	},
	(folders, callback) => {
		console.log(`${folders.length} folders`);

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
		console.log(`${documents.length} documents`);

		callback(null, folders, documents);
	},
	(folders, documents, callback) => {
		var s3 = new AWS.S3();

		callback(null, s3, folders, documents);
	},
	(s3, folders, documents, callback) => {
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
		async.eachLimit(folders, 15, (folder, callback) => {
			s3.putObject({
				Bucket: opts.bucket,
				Key: folder.href.self.slice(-36),
				Metadata: {
					'filepath': folder.path
				}
			}, (err, data) => {
				if (err) {
					return callback(err);
				}

				callback();
			});
		}, (err) => {
			if (err) {
				return callback(err);
			}

			callback(null, s3, folders, documents);
		});
	},
	(s3, folders, documents, callback) => {
		async.eachLimit(documents, 15, (doc, callback) => {
			var stream = new MemoryStream();

			SpringCM.document.download(doc, stream, (err) => {
				if (err) {
					return callback(err);
				}

				s2b(stream, (err, buffer) => {
					s3.putObject({
						Body: buffer,
						Bucket: opts.bucket,
						Key: doc.href.self.slice(-36),
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
				});
			});
		}, (err) => {
			if (err) {
				return callback(err);
			}
		});
	}
], (err) => {
	if (err) {
		return console.log(err);
	}
});
