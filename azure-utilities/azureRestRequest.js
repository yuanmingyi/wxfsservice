var https = require('https');
var http = require('http');
var fs = require('fs');
var util = require('util');
var format = util.format;
var crypto = require('crypto');
var parseXml = require('xml2js').parseString;

var config = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'utf8'));
var clientId = process.env.WEBJOBS_NAME || "wxfileservice";

function constructCanonicalizedHeaders(options) {
    var headers = options.headers;
    var msHeaders = [];
    for (var key in headers) {
        if (headers.hasOwnProperty(key) && key.toLowerCase().indexOf('x-ms-') === 0) {
            msHeaders.push(key.trim().toLowerCase() + ':' + headers[key].trim().replace(/\s{2,}/g, ' '));
        }
    }

    msHeaders.sort(function (h1, h2) {
        return h1.slice(0, h1.indexOf(':')).localeCompare(h2.slice(0, h2.indexOf(':')));
    });

    return msHeaders.join('\n');
}

function constructCanonicalizedResource(options) {
    var path = options.path;
    var queryStart = path.indexOf('?');
    var pathname = path;
    var query = [];
    if (queryStart !== -1) {
        pathname = path.slice(0, queryStart);
        query = path.slice(queryStart + 1).split('&');
    }

    var canonicalizedResourceString = "/" + config.account + pathname;

    for (var k = 0; k < query.length; k++) {
        var param = query[k];
        var separator = param.indexOf('=');
        var key = param.slice(0, separator);
        var value = param.slice(separator + 1);
        query[k] = key.toLowerCase() + '=' + value;
    }

    query.sort(function (q1, q2) {
        return q1.slice(0, q1.indexOf('=')).localeCompare(q2.slice(0, q2.indexOf('=')));
    });

    for (var k = 0; k < query.length; k++) {
        var param = query[k];
        var separator = param.indexOf('=');
        var key = param.slice(0, separator);
        var value = param.slice(separator + 1);
        query[k] = decodeURIComponent(key) + ':' + decodeURIComponent(value);
    }

    if (query.length > 0) {
        canonicalizedResourceString += '\n' + query.join('\n');
    }

    return canonicalizedResourceString;
}

var constructAzureOptions = exports._testOptionsConstructor = function (method, host, port, path, version, date, cid, secureKey, account, testing) {
    var options = {
        method: method,
        hostname: host,
        port: port,
        path: path,
        headers: {
            'x-ms-version': version,
            'x-ms-date': date,
            'x-ms-client-request-id': cid,
            'Authorization': 'SharedKey ' + account + ':',
            'Accept': 'application/json;odata=nometadata',
            'Accept-Charset': 'UTF-8'
        }
    };

    var canonicalizedHeaders = constructCanonicalizedHeaders(options);
    var canonicalizedResource = constructCanonicalizedResource(options);
    var stringToSign = format('%s\n\n\n\n\n\n\n\n\n\n\n\n%s\n%s', method, canonicalizedHeaders, canonicalizedResource);
    if (testing) {
        console.log(format('sign string:\n%s', util.inspect(stringToSign)));
    }

    // Encoding the Signature
    // Signature=Base64(HMAC-SHA256(UTF8(StringToSign)))
    var shahmac = crypto.createHmac("SHA256", new Buffer(secureKey, 'base64'));
    var signature = shahmac.update(stringToSign, 'utf-8').digest('base64');
    options.headers['Authorization'] += signature;

    return options;
}

// compose the REST API request headers to the azure storage,
// refer to https://msdn.microsoft.com/en-us/library/azure/dd179428.aspx
var azureRequest = exports.request = function (method, path, port, callback) {
    var protocol = (port === 443 ? https : http);
    var options = constructAzureOptions(
        method,
        config.host,
        port,
        path,
        config.version,
        new Date().toUTCString(),
        clientId,
        config.primaryKey,
        config.account,
        false);

    return protocol.request(options, callback);
}

exports.restApis = function (protocol) {
    var obj = {};
    var protocol = protocol || 'https';
    var port = (protocol.toLowerCase() === 'http' ? 80 : 443);
    var createApi = function (method, makePathFunction, responseHandler) {
        return function (params, userCallback) {
            var userCallback = userCallback || params;
            if (typeof params === 'function') {
                // params is optional, which means the first parameter can be callback
                userCallback = params;
                params = {};
            }
            var pathWithParams = makePathFunction(params);
            var req = azureRequest(method, pathWithParams, port, function (res) {
                responseHandler(res, userCallback);
            });

            req.on('error', function (err) {
                console.log(util.format('problem with request: %s\n%s', err.message, util.inspect(req.headers)));
            });

            req.end();
        };
    };

    // list all the containers
    // the user callback receives one paramter: the returned js object 
    obj.listContainers = createApi('GET', function (params) {
        var path = '/?comp=list';
        for (var k in params) {
            if (params.hasOwnProperty(k)) {
                path += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
            }
        }
        return path;
    }, function (res, callback) {
        var output = '';
        console.log(config.host + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            parseXml(output, function (err, result) {
                var result = result;
                if (err) {
                    console.log(format('parse xml data failed: %s', util.inpsect(err)));
                    result = undefined;
                } else {
                    result = result.EnumerationResults.Containers[0].Container || [];
                    for (var i = 0; i < result.length; i++) {
                        result[i].Name = result[i].Name[0];
                        result[i].Properties = result[i].Properties[0];
                        for (var key in result[i].Properties) {
                            if (result[i].Properties.hasOwnProperty(key)) {
                                result[i].Properties[key] = result[i].Properties[key][0];
                            }
                        }
                    }
                }
                if (!!callback) {
                    callback(result);
                }
            });
        });
    });

    // delete the specified container
    // the user callback receives a boolean indicated whether the operation is successful
    obj.deleteContainer = createApi('DELETE', function (params) {
        var containerName = params;
        var timeout = 60;
        if (typeof params === 'object') {
            containerName = params.name;
            timeout = params.timeOut;
        }
        return '/' + containerName + '?restype=container&timeout=' + timeout;
    }, function (res, callback) {
        var result = (res.statusCode === 202);
        res.on('end', function () {
            if (!!callback) {
                callback(result);
            }
        });
    });

    // list all the blobs in a specified container
    // the user callback receives one paramter: the returned js object
    obj.listBlobs = createApi('GET', function (params) {
        var path = '/' + params.container + '?restype=container&comp=list';
        for (var k in params) {
            if (params.hasOwnProperty(k) && k !== 'container') {
                path += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
            }
        }
        return path;
    }, function (res, callback) {
        var output = '';
        console.log(config.host + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            parseXml(output, function (err, result) {
                var result = result;
                if (err) {
                    console.log(format('parse xml data failed: %s', util.inpsect(err)));
                    result = undefined;
                } else {
                    result = result.EnumerationResults.Blobs[0].Blob || [];
                    for (var i = 0; i < result.length; i++) {
                        result[i].Name = result[i].Name[0];
                        result[i].Properties = result[i].Properties[0];
                        for (var key in result[i].Properties) {
                            if (result[i].Properties.hasOwnProperty(key)) {
                                result[i].Properties[key] = result[i].Properties[key][0];
                            }
                        }
                    }
                }
                if (!!callback) {
                    callback(result);
                }
            });
        });
    });

    // delete the specified blob in specified container
    // the user callback receives a boolean indicated whether the operation is successful
    obj.deleteBlob = createApi('DELETE', function (params) {
        var containerName = params.container;
        var blobName = params.blob;
        var timeout = params.timeOut || 60;
        return '/' + containerName + '/' + blobName;
    }, function (res, callback) {
        var result = (res.statusCode === 202);
        res.on('end', function () {
            if (!!callback) {
                callback(result);
            }
        });
    });


    obj.queryEntities = createApi('GET', function (params) {
        var path = '/' + params.table;
        var properties = '';

        if (params.partitionKey && params.rowKey) {
            path += "(PartitionKey='" + encodeURIComponent(params.partitionKey) + "',RowKey='" + encodeURIComponent(params.rowKey) + "')?$select=";
        } else {
            path += "()?filter=" + encodeURIComponent(params.filter) + "?$select=";
        }

        if (params.properties instanceof Array) {
            for (var i = 0; i < params.properties.length; i++) {
                if (i > 0) {
                    properties += ',';
                }
                properties += encodeURIComponent(params[i]);
            }
        } else {
            properties = '*';
        }

        return path + properties;
    }, function (res, callback) {
        var output = '';
        console.log(config.host + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            result = JSON.parse(output);
            if (!!callback) {
                callback(result.value);
            }
        });
    });

    return obj;
};

exports.httpRequest = function (method, path, callback) {
    azureRequest(method, path, 80, callback);
};

exports.httpsRequest = function (method, path, callback) {
    azureRequest(method, path, 443, callback);
}
