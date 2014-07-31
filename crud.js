// Some functions for inserting quickly (unack'ed writes) (Create)
// Preheating data to get it into memory (Read)
// Changing the data, with/without growth (Update)
// Removing data (Delete)

// First function is used to create a random set of data, the C in CRUD
// Defaults/Assumptions: _id index only, collection always called "data"
// Takes 3 arguments:
// numGB is the approximate data size to create in GiB (integer usually, but any number should work)
// dbName is the database to use (defaults to a collection called data)
// usePowerOf2 is a boolean to allow you to select the storage strategy

createData = function(numGB, dbName, usePowerOf2) {
    var db1 = db.getSiblingDB(dbName);
    // set powerOf2 per the boolean, but need to handle it differently if it currently exists or not 
    // NOTE that second option will turn it off for all new collections
    
    if(db1.data.findOne()){ 
        db1.runCommand({ collMod : "data", usePowerOf2Sizes : usePowerOf2 });
    } else {
        db1.adminCommand({ setParameter: 1, newCollectionsUsePowerOf2Sizes: usePowerOf2 });
    };
    
    // check the shell version, if 2.5+ set legacy mode for unacked writes (for speed)
    var shellVersion = version().split('.').map(Number);
    if ( shellVersion[0] > 2 ) {
	    db1.getMongo().forceWriteMode("legacy");
	} else if (shellVersion[0] == 2) {
		if (shellVersion[1] > 4) {
		db1.getMongo().forceWriteMode("legacy");
		}
	};

    // with the document we are using, 68 iterations of this loop will get you ~1033MiB of data (not uncluding indexes), so use that as a multiplier
    var startTime = new Date();
    
    for(var j = 0; j < (numGB * 68); j++){
        // going to create a big array of docs, then insert them
        var bigDoc = [];
        for(var i = 0; i < 66400; i++){  // 132800 gets pretty close to the max doc size but takes a bit too long to generate on my machine, leaving gaps, so divide by 2
            var randomNum = Math.random(); // generate a single random number per loop iteration
            var ranDate = new Date(Math.floor(1500000000000 * randomNum));
            // let's construct a random ObjectId based on the number, basically construct a string with the info we need and some randomness
            // first piece is 4 bytes (8 hex digits), need to pad with zeroes for low values (same with random end piece)
            // next pieces per the spec are 6 hex digits for the machine, 4 digits for the PID
            // instead we will insert 10 placeholder characters for expedience
            // then, round things out with 3 bytes of randomness per the spec, and use the increment on the loop to avoid collisions
            var ranString = (Math.floor(randomNum * 1500000000).toString(16)).pad(8, false, 0) + "adacefd123" + ((Math.floor(randomNum * 16710815) + i).toString(16)).pad(6, false, 0);
            // this one would be better, but too slow to generate:
            // var ranString = ((Math.floor(1500000000 * randomNum)).toString(16)).pad(8, false, "0") + db1.version().replace(/\./g, "") + "adacefd" + ((Math.floor(randomNum * 9920329) + i).toString(16)).pad(6, false, "0");
            var ranId = new ObjectId(ranString);
            // To explain the document:
            // _id and ranDate are both based on the same randomly generated date, but ranDate has millis and is a bit easier to parse
            // After that we add an integer, boolean and a small array with a string and a number (array is useful for growth later)
            bigDoc.push({_id : ranId, ranDate : ranDate, ranInt : NumberInt(randomNum * 1000000), ranBool : (randomNum < 0.5 ? true : false), smallArray : [randomNum, randomNum.toString()]});
        };
        db1.data.insert(bigDoc);
    
        if(j == (numGB * 34)){
            print("Approximately 50% done: " + (j * 66400) + " docs inserted in " + (new Date() - startTime)/1000 + " seconds");
        };
    
    };
    var timeTaken = ((new Date() - startTime)/1000);
    print("Run complete: " + (numGB * 68 * 66400) + " docs inserted in " + timeTaken + " seconds.  Average insertion rate: " + ((numGB * 136 * 33200)/timeTaken) + " docs per second");
    // clean up the write mode if altered at the top
    if(db1.getMongo().writeMode() == "legacy"){
	    db1.getMongo().forceWriteMode("commands"); 
    }    
};

// Sample runs of the createData script on a standalone mongod, both run on same 8 core Linux host, not IO bound 
// Single thread CPU for shell was close to max, as was database lock on mongod - results within margin of error for the versions:

// 2.6.3 - Run complete: 4515200 docs inserted in 148.452 seconds.  Average insertion rate: 30415.218387088084 docs per second
// 2.4.10 - Run complete: 4515200 docs inserted in 146.916 seconds.  Average insertion rate: 30733.20809169866 docs per second


// Next, the reads - we'll do this randomly across the set
// Takes 2 arguments:
// numGB is the approximate data size to create in GiB (integer usually, but any number should work)
// dbName is the database to use (defaults to a collection called data)

preHeatRandomData = function(numGB, dbName) {

// We will brute force this basically, the _id is indexed and we know how it was constructed
// The first 8 hex digits of the pseudo ObjectID we created have a maximum of 16^6 docs, but likely far less
// A bit of experimentation tells me that using all 8 digits is too slow (low hit rate)
// Even 6 digits is still only 256 second ranges and yielded an average of less than 2 docs per range in limited tests
// Hence, we will do a range query using the first 5 digits plus fixed strings to create the start/end of the range
// Thats 16^3 or 4096 secs, so not an unreasonable range to query in general (1GiB set tests yielded ~12 docs per range)
// Every time we do the range query, we will call explain, and then increment the results by the nscanned count
// 
// Note: decent chance there will be collisions, so may need to "oversubscribe" the amount of data to be touched

var docHits = 0;
var noHits = 0;
var iterations = 0;   // not really needed other than for stats

var db1 = db.getSiblingDB(dbName);

var startTime = new Date(); // time the loop
while(docHits < (5000000 * numGB)) {
	// the creation of the string is identical to the creation code
    var randomNum = Math.random();
    var ranString = (Math.floor(randomNum * 1500000000).toString(16)).pad(8, false, 0);
    // we just strip the last 3 characters to allow us to create ranges - 3 characters is only 4096 seconds
    ranString = ranString.substring(0, ranString.length - 3)
    var beginId = new ObjectId(ranString + "000adacefd123000000");
    var endId = new ObjectId(ranString + "fffadacefd123ffffff");                  
    // simple ranged query on _id with an explicit hint and an explain so we exhaust the cursor and get useful stats back
    var result = db1.data.find({_id : {$gte : beginId, $lte : endId}}).hint({_id : 1}).explain();
    if(result.nscanned > 0) { 
        docHits += result.nscanned; //increment by number of scanned if not empty
    } else {
        noHits++;  // record the lack of hits
    };
    iterations++; // total iterations
    // warn about low hit rates at each 250k no hit occurrences
	if((noHits % 250000) == 0  && noHits > 0){
        print("Warning: hit rate is poor - just passed " + noHits + " iterations with no hits (current hits doc hits are: " + docHits + " out of " + (5000000 * numGB) + " or " + docHits/(50000 * numGB) + "%).");
	};
};
var endTime = new Date();
// some info on the time taken, hit rate etc.
print(numGB + "GiB of data loaded (" + (numGB * 5000000) + " docs), took " + (endTime - startTime)/1000 + " seconds to complete (average: " + (numGB * 5000000)/((endTime - startTime)/1000) + " docs/sec)")
print(noHits + " queries hit 0 documents (" + (noHits*100)/iterations + "%) and there were " + iterations + " total iterations." );
print("Average number of docs scanned per iteration (hits only): " + (numGB * 5000000)/(iterations - noHits) );
};

// update docs, optionally making them grow (creates free list)

updateRandomData = function(numGB, dbName, growDocs){

// quick test shows that with powerOf2Sizes, need to add 9 ObjectIds to the smallArray to trigger a move
// so growing the docs will take a lot more updates in order to complete the run
// testing for a move is a little clunky until we get better write command stats, so pushing that to its own function for now
var db1 = db.getSiblingDB(dbName);
var updateHits = 0;
var growthOverhead = 0;
var startTime = new Date(); // time the loop
while(updateHits < (5000000 * numGB)){
    // we'll re-use the logic from the finds, create a range to look for a candidate document
    var randomNum = Math.random();
    var ranString = (Math.floor(randomNum * 1500000000).toString(16)).pad(8, false, 0);
    // we just strip the last 3 characters to allow us to create ranges - 3 characters is only 4096 seconds
    // this is looking pretty inefficient at finding data in a 2GB data set for testing, may need to increase the ranges
    ranString = ranString.substring(0, ranString.length - 3)
    var beginId = new ObjectId(ranString + "000adacefd123000000");
    var endId = new ObjectId(ranString + "fffadacefd123ffffff");
	var result = 0;
    // simple find on _id with a hint and next() to get the first doc off the cursor
	// loop until we have a valid result (in case of misses), and we will use the ranInt to not hit docs twice
	while(result == 0){ 
        result = db1.data.find({_id : {$gte : beginId, $lte : endId}, ranInt : {$lte : 1000000}}).hint({_id : 1}).next();
    }
    if(growDocs){
        growthOverhead += pushUntilMoved(dbName, result._id, false);
        db1.data.update({_id : result._id}, {$inc : {ranInt : 1000000}});
        updateHits++;
    } else {
	    db1.data.update({_id : result._id}, {$inc : {ranInt : 1000000}});
        updateHits++;
	}
}
var endTime = new Date();

if(growDocs){
    print("Updated " + updateHits + " docs in " + (endTime - startTime)/1000 + " seconds (avg: " + (5000000 * numGB)/((endTime - startTime)/1000) + " docs/sec. Growth required an average of " + (growthOverhead/updateHits) + " pushes to the array.");
} else {
    print("Updated " + updateHits + " docs in " + (endTime - startTime)/1000 + " seconds (avg: " + (5000000 * numGB)/((endTime - startTime)/1000) + " docs/sec.");
};

}

// this little function will take an ObjectID, then push new IDs to the smallArray until the document moves on disk
// verbose toggles information about old/new location and number of pushes required (will be more for powerOf2 docs)
// it's needed to provide the move functionality in the update function
pushUntilMoved = function(dbName, docID, verbose){
	var db1 = db.getSiblingDB(dbName);
	var currentLoc = db1.data.find({_id : docID}).showDiskLoc().next().$diskLoc;
	var newLoc = currentLoc;
	var pushes = 0;
	while((currentLoc.file == newLoc.file) && (currentLoc.offset == newLoc.offset)){
		db1.data.update({_id : docID}, {$push : {smallArray : new ObjectId()}});
		newLoc = db1.data.find({_id : docID}).showDiskLoc().next().$diskLoc;
		pushes++;
	}
	if(verbose){
		print("Old location: file: " + currentLoc.file + " offset: " + currentLoc.offset);
		print("New location: file: " + newLoc.file + " offset: " + newLoc.offset);
		print("Pushes required: " + pushes);
    }
	return pushes;
}


// delete docs, create holes and a free list

deleteRandomData = function(numGB, dbName){
	var db1 = db.getSiblingDB(dbName);
	var delHits = 0; 
	
	// this one is actually far more simple in 2.6 with the write results, so writing that first, may not bother with 2.4
 	var startTime = new Date(); // time the loop
	while(delHits < (5000000 * numGB)){
	// we'll re-use the logic from the finds/updates, create a range to look for a candidate document
	    var randomNum = Math.random();
	    var ranString = (Math.floor(randomNum * 1500000000).toString(16)).pad(8, false, 0);
	    ranString = ranString.substring(0, ranString.length - 3)
	    var beginId = new ObjectId(ranString + "000adacefd123000000");
	    var endId = new ObjectId(ranString + "fffadacefd123ffffff");
		var result = db1.data.remove({_id : {$gte : beginId, $lte : endId}}, 1); // just remove one doc at a time
		delHits += result.nRemoved;
    }
    var endTime = new Date();
	print("Removed " + delHits + " docs in " + (endTime - startTime)/1000 + " seconds (avg: " + (5000000 * numGB)/((endTime - startTime)/1000) + " docs/sec."); 	
	
}