var azureApis = require('./azure-utilities/azureRestRequest').restApis('https');
var util = require('util');
var fs = require('fs');
var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
var msPerHour = 3600000;
var msPerMinute = 60000;
var msPerSecond = 1000;
var expiredTimeSpan = parseInt(config.expiredPeriod) * msPerHour;   // milliseconds
var runInterval = config.runInterval * msPerMinute;    // milliseconds

function deleteIfExpired(containerName, blob) {
    var createdDate = new Date(blob.Properties['Last-Modified']);
    var currentDate = new Date();
    var name = blob.Name;
    var timeLeft = expiredTimeSpan - (currentDate - createdDate);

    console.log(util.format('checking %s/%s...', containerName, name));
    if (timeLeft <= 0) {
        console.log(util.format('%s/%s is expired! being deleted...', containerName, name));
        azureApis.deleteBlob({ container: containerName, blob: name }, function (result) {
            console.log(util.format('%s/%s delete %s', containerName, name, result ? 'succeeded' : 'failed'));
        });
    } else {
        var hours = timeLeft / msPerHour;
        var minutes = (timeLeft % msPerHour) / msPerMinute;
        var seconds = ((timeLeft % msPerHour) % msPerMinute) / msPerSecond;
        console.log(util.format('remaining life time: %d:%d:%d', Math.floor(hours), Math.floor(minutes), Math.floor(seconds)));
    }
}

//list all the container in the storage
function run() {
    console.log('start scanning...');
    azureApis.listContainers(function (result, containers) {
        if (!result) {
            console.log(util.format('request for Listing Containers fail:\n %s', containers));
        } else if (!!containers) {
            for (var i = 0; i < containers.length; i++) {
                // console.log(util.inspect(containers[i]));
                azureApis.listBlobs({ container: containers[i].Name }, (function (name) {
                    var containerName = name;
                    return function (result, blobs) {
                        if (!result) {
                            console.log(util.format('request for Listing Blobs fail:\n %s', blobs));
                        } else {
                            for (var j = 0; j < blobs.length; j++) {
                                deleteIfExpired(containerName, blobs[j]);
                            }
                            console.log(util.format('%d blobs in container %s', blobs.length, containerName));
                        }
                    };
                })(containers[i].Name));
            }

            console.log(util.format('total %d containers', containers.length));
        }
    });
    azureApis.queryEntities({
        table: config.userUploadsCountTable
    }, function (result, entities) {
        if (result) {
            console.log(util.format('Entries in table %s: \n%s', config.userUploadsCountTable, util.inspect(entities)));
        } else {
            console.log(util.format('request for querying entries in %s fail:\n %s', config.userUploadsCountTable, entities));
        }
    });
    azureApis.queryEntities({
        table: config.fileInfoTable,
        PartitionKey: '0cd245813b9dd7544883521a2057d00125500be2',
        RowKey: 'user'
    }, function (result, entities) {
        if (result) {
            console.log(util.format('Entries in table %s: \n%s', config.fileInfoTable, util.inspect(entities)));
        } else {
            console.log(util.format('request for querying entries in %s fail:\n %s', config.fileInfoTable, entities));
        }
    });

    setTimeout(run, runInterval);
    console.log(util.format('waiting for %d minutes to restart...', Math.floor(runInterval / msPerMinute)));
}

run();