// This is a simple function, which takes just two arguments:
// ns: a string representing the sharded namespace to be examined
// est: a boolean to determine whether or not to use the estimate option (recommended generally)

// It is called from the mongos like so:

// AllChunkInfo("database.collection", true);
// Currently the output is CSV, will add options for other output later

sh.printAllChunkInfo = function(ns, est) {
    var configDB = db.getSiblingDB("config");
    var chunks = configDB.chunks.find({ns: ns}).sort({min: 1});
    var totalChunks = 0;
    var totalSize = 0;
    var totalEmpty = 0;
    print("ChunkID,Shard,ChunkSize,ObjectsInChunk");
    chunks.forEach( function printChunkInfo(chunk) {
		var db1 = db.getSiblingDB(chunk.ns.split(".")[0]);
		var key = configDB.collections.findOne({_id: chunk.ns}).key;
		var res = db1.runCommand({ datasize: chunk.ns, keyPattern: key, min: chunk.min, max: chunk.max, estimate: est });
		print(chunk._id + "," + chunk.shard + "," + res.size + "," + res.numObjects);
		totalSize += res.size;
		totalChunks++;
		if (res.size == 0) {
			totalEmpty++;
		}
	} );
    print("*********** Summary Chunk Information ***********");
    print("Total Chunks: " + totalChunks);
    print("Average Chunk Size (bytes): " + (totalSize/totalChunks));
    print("Empty Chunks: " + totalEmpty);
    print("Average Chunk Size (non-empty): " + (totalSize/(totalChunks-totalEmpty)));
}
