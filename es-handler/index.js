var AWS = require('aws-sdk');
var path = require('path');

var attachAliases = function (tenant_id, es_object) {
    es_object.aliases = {};
    es_object.aliases[tenant_id] = {};
};

var getCoreTemplate = function () {
    return {
        "index_patterns": ["*"],
        "order": 1000,
        "mappings": {
            "_source": { "enabled": true },
            "properties": {
                "suggest": {
                    "type": "completion"
                },
                "extracted_keyphrases": {
                    "type": "keyword"
                },
                "extracted_entities": {
                    "type": "keyword"
                }
            }
        }
    };
};

var getElasticsearchRequest = function (method, es_path, es_object) {
    var creds = new AWS.EnvironmentCredentials('AWS');
    const endpoint = new AWS.Endpoint(process.env.ESEndpoint);

    var req = new AWS.HttpRequest(endpoint);
    req.method = method;
    req.path = es_path;
    req.region = process.env.ESAWSRegion;
    if (es_object) {
        req.body = JSON.stringify(es_object, null);
    }
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = 'application/json';

    var signer = new AWS.Signers.V4(req, 'es');
    signer.addAuthorization(creds, new Date());

    return req;
};

var sendElasticsearchRequest = function (req, on_success, on_error) {
    var send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function (httpResp) {
        var body = '';
        httpResp.on('data', function (chunk) {
            body += chunk;
        });
        httpResp.on('end', function (chunk) {
            console.log('Elasticsearch request is succeeded. Status - Response : ', httpResp.statusCode, body);
            on_success(httpResp.statusCode, body);
        });
    }, function (err) {
        console.log('Elasticsearch request is failed. Error : ', err);
        on_error(err);
    });

};

var putESCoreTemplate = function (callback) {
    var core_template = getCoreTemplate();
    console.log('Core template json:', core_template);
    var req = getElasticsearchRequest('PUT', path.join('/_template/', 'gss_core_template'), core_template);
    sendElasticsearchRequest(req, function (status) {
        console.log('Core template is created.');
        callback();
    }, function () {
        console.log('Core template creation is failed.');
    });
}

var verifyESCoreTemplate = function (callback) {
    var req = getElasticsearchRequest('HEAD', path.join('/_template/', 'gss_core_template'));
    sendElasticsearchRequest(req, function (status) {
        if (status === 200) {
            console.log('Core template exists. Skipping...');
            callback();
        } else {
            console.log('Core template does not exist. Creating...');
            putESCoreTemplate(callback);
        }
    }, function () {
        console.log('Core template check is failed.');
    });
}

var putESIndex = function (index_name, es_object, context) {
    var req = getElasticsearchRequest('PUT', path.join('/', index_name), es_object);
    sendElasticsearchRequest(req, function () {
        context.succeed('Index is created.');
    }, function () {
        context.fail('Index creation is failed.');
    });
};

exports.putNewIndex = (index_name, tenant_id, es_object, context) => {
    console.log('putNewIndex: ', index_name);

    verifyESCoreTemplate(function () {
        attachAliases(tenant_id, es_object);
        putESIndex(index_name, es_object, context);
    });
}
