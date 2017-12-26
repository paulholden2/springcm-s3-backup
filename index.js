const clcmd = require('command-line-commands');
const clarg = require('command-line-args');
const clusg = require('command-line-usage');
const backup = require('./backup.js');

const commands = [
	null,
	'backup',
	'restore'
];

const options = [
	{
		name: 'verbose',
		alias: 'v',
		description: 'Enables verbose logging of events and operations.',
		type: Boolean
	},
	{
		name: 'data-center',
		alias: 'd',
		description: 'The SpringCM data center for the account to use.',
		type: String
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

const { command, argv } = clcmd(commands, process.argv.slice(2, process.argv.length));

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

if (!command || opts.help) {
	return console.log(clusg(sections));
}

if (command === 'backup' && opts.bucket && opts.id && opts.secret && opts['data-center']) {
	backup(opts);
} else if (command === 'restore' && opts.bucket && opts.id && opts.secret && opts['data-center']) {
	console.log('Restore not supported yet');
} else {
	console.log(clusg(sections));
}
