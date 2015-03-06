var https = require('https');
var http = require('http');
var fs = require('fs');
var util = require('util');
var crypto = require('crypto');
var parseXml = require('xml2js').parseString;

var config = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'utf8'));
var clientId = process.env.WEBJOBS_NAME || "wxfileservice";

var __debug = process.env.__DEBUG;

function constructCanonicalizedHeaders(options) {
    var headers = options.headers;
    var msHeaders = [];
    for (var key in headers) {
        if (headers.hasOwnProperty(key) && key.toLowerCase().indexOf('x-ms-') === 0) {
            // console.log(util.format('key: %s, value: %s', key, headers[key]));
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

function constructHeadersWithoutAuth(version, date, cid, moreHeaders) {
    var headers = {
        'x-ms-version': version,
        'x-ms-date': date,
        'x-ms-client-request-id': cid,
        'Accept': 'application/json;odata=nometadata',
        'Accept-Charset': 'UTF-8'
    };

    for (var key in moreHeaders) {
        if (moreHeaders.hasOwnProperty(key)) {
            headers[key] = moreHeaders[key];
        }
    }

    return headers;
}

var generateSignature = exports.generateSignature = function (secureKey, stringToSign) {
    // Encoding the Signature
    // Signature=Base64(HMAC-SHA256(UTF8(StringToSign)))
    var shahmac = crypto.createHmac("SHA256", new Buffer(secureKey, 'base64'));
    return shahmac.update(stringToSign, 'utf-8').digest('base64');
};

function constructAuthorizationHeader(cr, keyName, account, secureKey, verb, headers, canonicalizedHeaders, canonicalizedResource) {
    var stringToSign = '';
    for (var i = 0; i < cr.length; i++) {
        var key = cr[i];
        if (key === 'verb') {
            stringToSign += verb + '\n';
        } else {
            stringToSign += (headers[key] || '') + '\n';
        }
    }

    if (canonicalizedHeaders) {
        stringToSign += canonicalizedHeaders + '\n';
    }

    stringToSign += canonicalizedResource;
    if (__debug) {
        console.log(util.format('sign string:\n%s', util.inspect(stringToSign)));
    }

    var signatuare = generateSignature(secureKey, stringToSign);
    return util.format('%s %s:%s', keyName, account, signatuare);
}

var makeAuthorizationHeader = function (lite, account, secureKey, verb, headers, canonicalizedHeaders, canonicalizedResource) {
    var cr = ['verb', 'Content-Encoding', 'Content-Language', 'Content-Length', 'Content-MD5', 'Content-Type', 'Date', 'If-Modified-Since', 'If-Match', 'If-None-Match', 'If-Unmodified-Since', 'Range'];
    if (lite) {
        cr = ['verb', 'Content-MD5', 'Content-Type', 'Date'];
    }
    var keyName = lite ? 'SharedKeyLite' : 'SharedKey';
    return constructAuthorizationHeader(cr, keyName, account, secureKey, verb, headers, canonicalizedHeaders, canonicalizedResource);
};

var makeTableAuthorizationHeader = function (lite, account, secureKey, verb, headers, canonicalizedResource) {
    var cr = lite ? ['x-ms-date'] : ['verb', 'Content-MD5', 'Content-Type', 'x-ms-date'];
    var keyName = lite ? 'SharedKeyLite' : 'SharedKey';
    return constructAuthorizationHeader(cr, keyName, account, secureKey, verb, headers, '', canonicalizedResource);
}

var constructAzureOptions = exports._testOptionsConstructor = function (method, host, port, path, version, date, cid, secureKey, account, lite) {
    var options = {
        method: method,
        hostname: host,
        port: port,
        path: path,
        headers: constructHeadersWithoutAuth(version, date, cid)
    };

    var canonicalizedHeaders = constructCanonicalizedHeaders(options);
    var canonicalizedResource = constructCanonicalizedResource(options);
    options.headers['Authorization'] = makeAuthorizationHeader(lite, account, secureKey, method, options.headers, canonicalizedHeaders, canonicalizedResource);

    return options;
};

var constructAzureTableOptions = exports._testTableOptionsConstructor = function (method, host, port, path, version, date, cid, odataVersion, maxOdataVersion, secureKey, account, lite) {
    var options = {
        method: method,
        hostname: host,
        port: port,
        path: path,
        headers: constructHeadersWithoutAuth(version, date, cid, { DataServiceVersion: odataVersion, MaxDataServiceVersion: maxOdataVersion })
    };

    var canonicalizedResource = constructCanonicalizedResource(options);
    options.headers['Authorization'] = makeTableAuthorizationHeader(lite, account, secureKey, method, options.headers, canonicalizedResource);

    return options;
};

var getTypeFromHost = function (host) {
    var firstDot = host.indexOf('.');
    return host.slice(firstDot + 1, host.indexOf('.', firstDot + 1));
};

// compose the REST API request headers to the azure storage,
// refer to https://msdn.microsoft.com/en-us/library/azure/dd179428.aspx
var azureRequest = exports.request = function (method, host, path, port, callback) {
    var protocol = (port === 443 ? https : http);
    var type = getTypeFromHost(host);
    var options = null;
    if (type === 'table') {
        options = constructAzureTableOptions(
            method,
            host,
            port,
            path,
            config.version,
            (new Date()).toUTCString(),
            clientId,
            config.dataServiceVersion,
            config.maxDataServiceVersion,
            config.primaryKey,
            config.account,
            true);
    } else {
        options = constructAzureOptions(
            method,
            host,
            port,
            path,
            config.version,
            (new Date()).toUTCString(),
            clientId,
            config.primaryKey,
            config.account,
            false);
    }

    if (__debug === 'trace') {
        console.log(util.format('request option:\n%s', util.inspect(options)));
    }

    return protocol.request(options, callback);
};

exports.restApis = function (protocol) {
    var obj = {};
    var protocol = protocol || 'https';
    var port = (protocol.toLowerCase() === 'http' ? 80 : 443);
    var createApi = function (method, host, makePathFunction, responseHandler) {
        return function (params, userCallback) {
            var userCallback = userCallback || params;
            if (typeof params === 'function') {
                // params is optional, which means the first parameter can be callback
                userCallback = params;
                params = {};
            }
            var pathWithParams = makePathFunction(params);
            var req = azureRequest(method, host, pathWithParams, port, function (res) {
                responseHandler(res, userCallback);
            });

            req.on('error', function (err) {
                console.log(util.format('problem with request: %s\n%s', err.message, util.inspect(req.headers)));
            });

            req.end();
        };
    };

    var createBlobApi = function (method, makePathFunction, responseHandler) {
        return createApi(method, config.blobHost, makePathFunction, responseHandler);
    };

    var createTableApi = function (method, makePathFunction, responseHandler) {
        return createApi(method, config.tableHost, makePathFunction, responseHandler);
    };

    var createQueueApi = function (method, makePathFunction, responseHandler) {
        return createApi(method, config.queueHost, makePathFunction, responseHandler);
    };

    // list all the containers
    // the user callback receives two parameters: 1. the request result (true for success and false for failure) 2. the object include all the containers
    obj.listContainers = createBlobApi('GET', function (params) {
        var path = '/?comp=list';
        for (var k in params) {
            if (params.hasOwnProperty(k)) {
                path += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
            }
        }
        return path;
    }, function (res, callback) {
        var output = '';
        console.log(config.blobHost + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            if (res.statusCode !== 200) {
                if (!!callback) {
                    callback(false, output);
                }
                return;
            }

            parseXml(output, function (err, result) {
                var result = result;
                if (err) {
                    console.log(util.format('parse xml data failed: %s', util.inpsect(err)));
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
                    callback(!err, result);
                }
            });
        });
    });

    // delete the specified container
    // the user callback receives a boolean indicated whether the operation is successful
    obj.deleteContainer = createBlobApi('DELETE', function (params) {
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
    // the user callback receives two parameters: 1. the request result (true for success and false for failure) 2. the object include the blobs in the container
    obj.listBlobs = createBlobApi('GET', function (params) {
        var path = '/' + params.container + '?restype=container&comp=list';
        for (var k in params) {
            if (params.hasOwnProperty(k) && k !== 'container') {
                path += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
            }
        }
        return path;
    }, function (res, callback) {
        var output = '';
        console.log(config.blobHost + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            if (res.statusCode !== 200) {
                if (!!callback) {
                    callback(false, output);
                }
                return;
            }

            parseXml(output, function (err, result) {
                var result = result;
                if (err) {
                    console.log(util.format('parse xml data failed: %s', util.inpsect(err)));
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
                    callback(!err, result);
                }
            });
        });
    });

    // delete the specified blob in specified container
    // the user callback receives a boolean indicated whether the operation is successful
    obj.deleteBlob = createBlobApi('DELETE', function (params) {
        var containerName = params.container;
        var blobName = params.blob;
        var timeout = params.timeOut || 60; // seconds
        return '/' + containerName + '/' + blobName + '?timeout=' + timeout;
    }, function (res, callback) {
        var result = (res.statusCode === 202);
        res.on('end', function () {
            if (!!callback) {
                callback(result);
            }
        });
    });

    obj.getBlobProperties = createBlobApi('HEAD', function (params) {
        var containerName = params.container;
        var blobName = params.blob;
        var timeout = params.timeOut || 60; // seconds
        return '/' + containerName + '/' + blobName + '?timeout=' + timeout;
    }, function (res, callback) {
        res.on('end', function () {
            var properties = null;
            if (res.statusCode === 200) {
                properties = res.headers;
            }
            if (!!callback) {
                callback(properties);
            }
        });
    });

    // the user callback receives two parameters: 1. the query result (true for success and false for failure) 2. the object include the result entities
    obj.queryEntities = createTableApi('GET', function (params) {
        var path = '/' + params.table;
        var query = '';

        if (params.PartitionKey && params.RowKey) {
            path += "(PartitionKey='" + encodeURIComponent(params.PartitionKey) + "',RowKey='" + encodeURIComponent(params.RowKey) + "')";
        } else {
            path += "()";
        }

        if (typeof params.query === 'object') {
            for (var key in params.query) {
                if (params.query.hasOwnProperty(key)) {
                    if (query === '') {
                        query = '?';
                    } else {
                        query += '&';
                    }

                    query += key + '=' + encodeURIComponent(params.query[key]);
                }
            }
        }

        return path + query;
    }, function (res, callback) {
        var output = '';
        console.log(config.tableHost + ':' + res.statusCode);
        res.setEncoding('utf8');

        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            if (res.statusCode !== 200) {
                if (!!callback) {
                    callback(false, output);
                }
                return;
            }

            result = JSON.parse(output);
            if (!!callback) {
                callback(true, result);
            }
        });
    });

    obj.deleteEntity = createTableApi('DELETE', function (params) {
        var table = params.table;
        var partitionKey = params.partitionKey;
        var rowKey = params.rowKey;
        return util.format("/%s(PartitionKey='%s', RowKey='%s')", partitionKey, rowKey);
    }, function (res, callback) {
        var result = (res.statusCode === 204);
        res.on('end', function () {
            if (!!callback) {
                callback(result);
            }
        });
    });

    return obj;
};

exports.httpRequest = function (method, host, path, callback) {
    azureRequest(method, host, path, 80, callback);
};

exports.httpsRequest = function (method, host, path, callback) {
    azureRequest(method, host, path, 443, callback);
};
