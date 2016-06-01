// console.log(__dirname);
var pond = require('../../pond')
var myArgs = process.argv.slice(2);

switch(myArgs[0]) {
    case 'ping':
        console.log('pong');
        break;
    default:
        console.log('did not get valid args')
        process.exit(-1)
}

process.exit(0)
