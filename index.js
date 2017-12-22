const dotenv = require('dotenv');
const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');

dotenv.config();

function subfolders(folder, callback) {
	SpringCM.folder.folders(folder, (err, folders) => {
		if (err) {
			return callback(err);
		}

		console.log(`${folder.name} subfolders: ${folders.length}`);

		if (folders.length > 0) {
			async.eachSeries(folders, (folder, callback) => {
				subfolders(folder, (err, folderlist) => {
					if (err) {
						return callback(err);
					}

					folders = folders.concat(folderlist);
					callback();
				});
			}, (err) => {
				if (err) {
					return callback(err);
				}

				callback(null, folders);
			});
		} else {
			callback(null, []);
		}
	});
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
		subfolders(root, (err, folderlist) => {
			if (err) {
				return callback(err);
			}

			callback(null, folderlist);
		});
	},
	(folderlist, callback) => {
		console.log(folderlist.length);
		callback();
	}
], (err) => {
	if (err) {
		return console.log(err);
	}
});
