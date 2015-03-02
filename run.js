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
    azureApis.listContainers(function (containers) {
        if (!!containers) {
            for (var i = 0; i < containers.length; i++) {
                // console.log(util.inspect(containers[i]));
                azureApis.listBlobs({ container: containers[i].Name }, function (blobs) {
                    for (var j = 0; j < blobs.length; j++) {
                        deleteIfExpired(containers[i].Name, blobs[j]);
                    }
                });
                console.log(util.format('%d blobs in container %s', blobs.length, containers[i].Name))
            }

            console.log(util.format('total %d containers', containers.length));
        }
        console.log(util.format('waiting for %d minutes to restart...', Math.floor(runInterval / msPerMinute)));
        setTimeout(run, runInterval);
    });
}

run();