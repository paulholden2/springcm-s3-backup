// Put an object into S3. If the contents of buffer are sufficiently large,
// a multipart upload is used.
const async = require('async');

module.exports = (s3, doc, key, buffer, callback) => {
	const maxsz = 1024 * 1024 * 10;

	console.log(`Uploading ${key}`);
	console.log(`${buffer.length} bytes (~${(buffer.length / 1024).toFixed(3)} KiB)`);

	if (buffer.length < maxsz) {
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
		var partNumbers = [];

		for (var i = 0; i < Math.ceil(buffer.length / maxsz); ++i) {
			partNumbers.push(i + 1);
		}

		async.waterfall([
			// Create multipart
			(callback) => {
				console.log(`Creating multipart upload for ${key}`);

				s3.createMultipartUpload({
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

					callback(null, data.UploadId);
				});
			},
			// Do part uploads
			(uploadId, callback) => {
				async.map(partNumbers, (part, callback) => {
					s3.uploadPart({
						Body: buffer.slice((part - 1) * maxsz, part * maxsz),
						Bucket: process.env.S3_BUCKET,
						Key: key,
						PartNumber: part,
						UploadId: uploadId
					}, (err, data) => {
						if (err) {
							return callback(err);
						}

						callback(null, {
							ETag: data.ETag,
							PartNumber: part
						});
					});
				}, (err, parts) => {
					if (err) {
						return callback(err);
					}

					callback(null, uploadId, parts);
				});
			},
			// Complete multipart
			(uploadId, parts, callback) => {
				s3.completeMultipartUpload({
					Bucket: process.env.S3_BUCKET,
					Key: key,
					MultipartUpload: {
						Parts: parts
					},
					UploadId: uploadId
				}, (err, data) => {
					if (err) {
						return callback(err);
					}

					console.log(`Completed multipart upload for ${key}`);

					callback();
				});
			}
		], (err) => {
			if (err) {
				return callback(err);
			}

			callback();
		});
	}
}
