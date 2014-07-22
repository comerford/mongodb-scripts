// 

// first function is used to create a random set of data, the C in CRUD
// takes 3 arguments:
// numGB is the approximate data size to create in GiB (integer usually, but any number should work)
// dbName is the database to use (defaults to a collection called data)
// usePowerOf2 is a boolean to allow you to select the storage strategy

createData = function(numGB, dbName, usePowerOf2) {
    db1 = db.getSiblingDB(dbName);
    // set powerOf2 per the boolean, but need to handle it differently if it currently exists or not 
    // NOTE that second option will turn it off for all new collections
    
    if(db1.data.findOne()){ 
        db1.runCommand({ collMod : "data", usePowerOf2Sizes : usePowerOf2 });
    } else {
        db1.adminCommand({ setParameter: 1, newCollectionsUsePowerOf2Sizes: usePowerOf2 });
    };
    
    // with the document we are using, 136 iterations of this loop will get you ~1033MiB of data (not uncluding indexes), so use that as a multiplier
    var startTime = new Date();
    
    for(var j = 0; j < (numGB * 136); j++){
        // going to create a big array of docs, then insert them
        var bigDoc = [];
        for(var i = 0; i < 33200; i++){  // 132800 gets pretty close to the max doc size but takes too long to generate, leaving gaps, so divide by 4
            var randomNum = Math.random(); // generate a single random number per loop iteration
            var ranDate = new Date(Math.floor(1500000000000 * randomNum));
            // let's construct a random ObjectId based on the number, basically construct a string with the info we need and some randomness
            // first piece is 4 bytes (8 hex digits), need to pad with zeroes for low values (same with random end piece)
            // next pieces per the spec are 6 hex digits for the machine, 4 digits for the PID
            // instead we will insert 10 placeholder characters for expedience
            // then, round things out with 3 bytes of randomness per the spec, and use the increment on the loop to avoid collisions
            var ranString = (Math.floor(randomNum * 1500000000).toString(16)).pad(8, false, 0) + "adacefd123" + ((Math.floor(randomNum * 16743015) + i).toString(16)).pad(6, false, 0);
            // this one would be better, but too slow to generate:
            // var ranString = ((Math.floor(1500000000 * randomNum)).toString(16)).pad(8, false, "0") + db1.version().replace(/\./g, "") + "adacefd" + ((Math.floor(randomNum * 9920329) + i).toString(16)).pad(6, false, "0");
            var ranId = new ObjectId(ranString);
            // To explain the document:
            // _id and ranDate are both based on the same randomly generated date, but ranDate has millis and is a bit easier to parse
            // After that we add an integer, boolean and a small array with a string and a number (array is useful for growth later)
            bigDoc.push({_id : ranId, ranDate : ranDate, ranInt : Math.floor(randomNum * 1000000), ranBool : (randomNum < 0.5 ? true : false), smallArray : [randomNum, randomNum.toString()]});
        };
        db1.data.insert(bigDoc);
    
        if(j == (numGB * 68){
            print("Approximately 50% done: " + (j * 34200) + " docs inserted in " + (new Date() - StartTime)/1000 + " seconds");
        };
    
    };
    var timeTaken = ((new Date() - startTime)/1000);
    print("Run complete: " + (numGB * 136 * 33200) + " docs inserted in " + timeTaken + " seconds.  Average insertion rate: " + ((numGB * 136 * 33200)/timeTaken) + " docs per second");    
};

// Next, the reads - we'll do this randomly across the set

preHeatRandomData = function(numGB, dbName) {
 
successfulRuns = 0;
unsuccessfulRuns = 0;  
startTime = new Date();

while(successfulRuns < 85) {
    x = Math.floor(Math.random() * diff);
    beginId = new Date(0);
    endId = new Date(1500000000000);
    var result = db.data.find({_id : {$gte : beginId, $lte : endId}}).explain();
    if(result.nscanned > 0) {
        successfulRuns++;
    } else {
        unsuccessfulRuns++;
    }
}

endTime = new Date();
print("1GB of data loaded, took " + (successfulRuns + unsuccessfulRuns) + " runs and " + (endTime - startTime)/1000 + " seconds to complete" );

};

// update docs, make them grow

// delete docs, create holes
