// This is a simple function, which takes just two arguments:
// ns: a string representing the sharded namespace to be examined
// est: a boolean to determine whether or not to use the estimate option (recommended generally)

// It is called from the mongos like so:

// AllChunkInfo("database.collection", true);
// Currently the output is CSV, will add options for other output later

sh.printAllChunkInfo = function(ns, est) {
	var configDB = db.getSiblingDB("config");
	var chunks = configDB.chunks.find({ns: ns}).sort({min: 1});
	var key = configDB.collections.findOne({_id: ns}).key;
	var total = { chunks: 0, objs: 0, size: 0, empty: 0 };
	var shards = {};
	configDB.shards.find().toArray().forEach( function (shard) {
		shards[shard._id] = { chunks: 0, objs: 0, size: 0, empty: 0 };
	} );
	print("ChunkID,Shard,ChunkSize,ObjectsInChunk");
	chunks.forEach( function printChunkInfo(chunk) {
		var res = db.getSiblingDB(chunk.ns.split(".")[0]).runCommand({ datasize: chunk.ns, keyPattern: key, min: chunk.min, max: chunk.max, estimate: est });
		print(chunk._id + "," + chunk.shard + "," + res.size + "," + res.numObjects);
		(function(stats) {
			for (stat in stats) {
				stats[stat].chunks++;
				stats[stat].objs += res.numObjects;
				stats[stat].size += res.size;
				if (res.size == 0) stats[stat].empty++;
			}
		})( [ total, shards[chunk.shard] ] );
	} );

	function printStats(s, indent) {
		print(indent + "Total Chunks: " + s.chunks + " (" + s.empty + " empty)");
		print(indent + "Total Size: " + s.size + " bytes");
		print(indent + "Average Chunk Size: " + (s.size/s.chunks) + " bytes");
		print(indent + "Average Non-empty Chunk Size: " + (s.size/(s.chunks-s.empty)) + " bytes");
	}

	print("");
	print("*********** Summary Information ***********");
	printStats(total, "");

	print("");
	print("*********** Per-Shard Information ***********");
	for (shard in shards) {
		var s = shards[shard];
		print("Shard " + shard + ":");
		printStats(shards[shard], "    ");
	}
}
