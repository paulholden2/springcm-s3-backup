const AWS = require('aws-sdk');
const SpringCM = require('springcm-node-sdk');
const async = require('async');

async.waterfall([
	(callback) => {
		callback();
	}
], (err) => {
	if (err) {
		return console.log(err);
	}
});
