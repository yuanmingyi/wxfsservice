var azureApis = require('./azure-utilities/azureRestRequest').restApis('https');
var tableInfo = require('../../../../../app/tableDef');
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

function binarySearch(arr, obj, compare) {
    if (typeof compare !== 'function') {
        compare = function (obj, ele) {
            return obj - ele;
        }
    }

    var low = 0, high = arr.length, mid;
    while (low < high) {
        mid = Math.floor((low + high) / 2);
        var res = compare(obj, arr[mid]);
        if (res < 0) {
            high = mid;
        } else if (res > 0) {
            low = mid + 1;
        } else {
            return mid;
        }
    }

    return -1;
}

//list all the container in the storage
function run() {
    console.log('start scanning...');


    azureApis.queryEntities({
        table: tableInfo.tableName
    }, function (result, entities) {
        if (!result) {
            logger.error("query table failed:\n %s", util.inspect(entities));
            return;
        }

        var dateNow = new Date();
        entities.sort(function (entry1, entry2) {
            return entry1.RowKey - entry2.RowKey;
        });

        // verify if there are blobs not in the table
        azureApis.listContainers(function (result, containers) {
            if (!result) {
                console.log(util.format('request for Listing Containers fail:\n %s', containers));
            } else if (!!containers) {
                containers.forEach(function (container) {
                    azureApis.listBlobs({ container: container.Name }, function (result, blobs) {
                        blobs.forEach(function (blob) {
                            // search the corresponding table entry
                            var found = binarySearch(entities, blob, function (blob, entity) {
                                // compare the hashcode
                                return blob.Name - entity.RowKey;
                            });

                            if (found === -1 || dateNow - new Date(entities[found].CreateDate) > expiredTimeSpan) {
                                // not found the entry or the file is expired
                                azureApis.deleteBlob({ container: container.Name, blob: blob.Name }, function (result) {
                                    if (result) {
                                        console.log(util.format('file blob %s delete succeeded', blob.Name));
                                    } else {
                                        console.log(util.format('file blob %s delete failed', blob.Name));
                                    }
                                });

                                if (found) {
                                    var entity = entities[found];
                                    azureApis.deleteEntity({ table: tableInfo.tableName, partitionKey: entity.PartitionKey, rowKey: entity.RowKey }, function (result) {
                                        if (result) {
                                            console.log(util.format('entity %s delete succeeded', util.inspect(entity)));
                                        } else {
                                            console.log(util.format('entity %s delete failed', util.inspect(entity)));
                                        }
                                    });
                                }
                            }
                        });

                        console.log(util.format('%d blobs in container %s', blobs.length, container.Name));
                    });
                });
                console.log(util.format('total %d containers', containers.length));
            }
        });

        entities.forEach(function (entity) {
            console.log(util.format('found entity:\n%s', util.inspect(entity)));
            azureApis.getBlobProperties({
                container: entity.FilePath,
                blob: entity.RowKey
            }, function (headers) {
                if (!headers) {
                    console.log('file %s (%s) is not found, to be deleted', entity.RowKey, entity.FileName);
                    azureApis.deleteEntity({ table: tableInfo.tableName, partitionKey: entity.PartitionKey, rowKey: entity.RowKey }, function (result) {
                        if (result) {
                            console.log(util.format('entity %s delete succeeded', util.inspect(entity)));
                        } else {
                            console.log(util.format('entity %s delete failed', util.inspect(entity)));
                        }
                    });
                }
            });
        });

        setTimeout(run, runInterval);
        console.log(util.format('waiting for %d minutes to restart...', Math.floor(runInterval / msPerMinute)));
    });
}

run();