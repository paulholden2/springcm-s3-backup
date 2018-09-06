const path = require('path');
const dotenv = require('dotenv');
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
    name: 'config',
    alias: 'c',
    description: 'The path to the .env file to use.',
    type: String
  },
  {
    name: 'no-log',
    description: 'Don\'t log backup to Zoho Reports',
    type: Boolean
  },
  {
    name: 'overwrite',
    alias: 'o',
    description: 'Always update content in S3, even if it is already recent',
    type: Boolean
  },
  {
    name: 'parallel',
    alias: 'p',
    description: 'How many concurrent requests to run. Must be >= 1',
    defaultValue: 10,
    type: Number
  },
  {
    name: 'help',
    alias: 'h',
    description: 'Displays this usage dialog.',
    type: Boolean
  }
];

const cmd = clcmd(commands, process.argv.slice(2, process.argv.length));
const command = cmd.command;
const argv = cmd.argv;

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

if (!command || opts.help || opts.parallel < 1) {
  return console.log(clusg(sections));
}

if (opts.config) {
  dotenv.config({
    path: path.join(__dirname, opts.config)
  });
}

if (command === 'backup') {
  backup(opts);
} else if (command === 'restore') {
  console.log('Restore not supported yet');
} else {
  console.log(clusg(sections));
}
