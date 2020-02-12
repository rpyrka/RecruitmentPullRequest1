var es_handler = require('eshandler');
var validation_handler = require('validationhandler');

console.log('Initializing AWS Lambda Function');

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log('Received event: ', JSON.stringify(event, null, 2));
    var operation = event.operation; // CREATEINDEX, UPDATEINDEX, DELETEINDEX, REINDEX, CREATETEMPLATE, UPDATETEMPLATE
    var index_name = event.indexName;
    var tenant_id = event.tenantId;
    var es_object = event.ESObject;
    var validationResponse = validation_handler.validateOperation(operation, es_object);
    if(!validationResponse.isValid){
        context.fail(validationResponse.message);
        return;
    }
    es_handler.putNewIndex(index_name, tenant_id, es_object, context);
};
