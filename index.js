const http = require('http');
const {EventProcessorHost, delay} = require("@azure/event-processor-host");
var sql = require('mssql');

var config = {
    server: "13.92.171.219",
    user: "SA",
    password: "BlkAware@1",
    database: "awareiot"
};
  
//Event Hub Connection
const storageCS = "DefaultEndpointsProtocol=https;AccountName=awarestorageaccount;AccountKey=EHgHKFqRk4LqfUYw/ddQoGjJIVjHOK8UAH9EurS9heVgDzPPYuJc4BLt3/Y2FFvX/i+jwO4wOn3sdINL9C42Vg==;EndpointSuffix=core.windows.net";
const ehCS = "Endpoint=sb://awareiotdata.servicebus.windows.net/;SharedAccessKeyName=mssql;SharedAccessKey=EkbeTndGW5o+O2Pf/IZgUzq6ce4E0sQGbnSbna6nrMs=;EntityPath=iotsamples";
const storageContainerName = "mssql-eventhubstorage";

var msg;
var records;
let table;


//Function to initialize sql table for bulk data insertion
function init() {

    //console.log("Starting here");
    var table = new sql.Table('samples')
    table.create = true
  
    table.columns.add('deviceid', sql.NVarChar('max'), {nullable: true})
    table.columns.add('gatewayid',sql.NVarChar('max'), {nullable: true})
    table.columns.add('gatewaytime',sql.DateTime , {nullable: true})
    table.columns.add('iothubtime',sql.DateTime , {nullable: true})
    table.columns.add('outputtype',sql.Int, {nullable: true})
    table.columns.add('outputvalue',sql.Float, {nullable: true})
    table.columns.add('solutionid',sql.BigInt, {nullable: true})
    table.columns.add('solutiontypeid',sql.BigInt, {nullable: false})
    table.columns.add('deploymenttargetid',sql.BigInt, {nullable: false})
    table.columns.add('buildingid',sql.BigInt, {nullable: true})
  
    return table;
  }
  
  //Connect to MS SQL Server and initialize table
  sql.connect(config, async  function (err) {
  table = init();
  request = new sql.Request();
  
  
  //Function gets data from Azure Event Hub and converts to JSON format
  async function main() {
    // Create the Event Processor Host
    const eph = EventProcessorHost.createFromConnectionString(
      EventProcessorHost.createHostName("mssql"),
      storageCS,
      storageContainerName,
      ehCS,
      {
        eventHubPath: 'iotsamples',
        onEphError: (error) => {
          console.log("This handler will notify you of any internal errors that happen " +
          "during partition and lease management: %O", error);
        }
      }
    );
    let count = 0;
    // Message event handler
    const onMessage = async (context/*PartitionContext*/, data /*EventData*/) => {
  
      msg=JSON.stringify((data.body).toString('utf-8'));
      //console.log(msg);
      jsonmsg = JSON.parse(msg);
  
      try{
        records = jsonmsg.split('\r\n').map(function (record) {
          return (record);
        })
      }
      catch(err){
        console.log(err);
      }
  
      if(records === undefined){
        records = data.body;
      }
  
      if(records.length==1){
        var promise = new Promise(function(err, res) {
          table = init();
          qry=data.body;
  
          table.rows.add(qry.deviceid, qry.gatewayid, new Date(qry.createdate), new Date(qry.iothubenqueuedate), qry.outputtype, qry.outputvalue, qry.solutionid, qry.solutiontypeid,
            qry.deploymenttargetid, qry.buildingid)
  
          request.bulk(table, (err, result) => {
            //console.log(result);
          })
        });
      }
  
      else{
        var promise = new Promise(function(err, res) {
        table = init();
        for (var i=0; i<records.length; i++){
          qry = JSON.parse(records[i]);
          table.rows.add(qry.deviceid, qry.gatewayid, new Date(qry.createdate), new Date(qry.iothubenqueuedate), qry.outputtype, qry.outputvalue, qry.solutionid, qry.solutiontypeid,
            qry.deploymenttargetid, qry.buildingid)
          }
          request.bulk(table, (err, result) => {
            //console.log(result);
          })
        });
      }
  
      count++;
      // let us checkpoint every 1000th message that is received across all the partitions.
      if (count % 1000 === 0) {
          return await context.checkpoint();
      }
    };
    // Error event handler
    const onError = (error) => {
      console.log(">>>>> Received Error: %O", error);
    };
    // start the EPH
    await eph.start(onMessage, onError);
    //After some time let' say 2 minutes
    //await delay(120);
    // This will stop the EPH.
    //await eph.stop();
  }
  
  //Run the function to get data from Event Hub
    main().catch((err) => {
      console.log(err);
    });
  });
  


// const server = http.createServer((request, response) => {
//     response.writeHead(200, {"Content-Type": "text/plain"});
//     response.end("Sending Message to MS SQL!");
// });

// const port = process.env.PORT || 1337;
// server.listen(port);

// console.log("Server running at http://localhost:%d", port);
