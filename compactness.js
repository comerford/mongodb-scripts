// original credit for this goes to https://github.com/achille
// compactness() calculates how closely the resulting documents are located together
// It counts the size of the d vs size of the unique pages they reside on
   
function compactness(collection, query, limit) {
    "use strict";
    Object.size = function(o) {
        var size = 0, key;
        for (key in o) { if (o.hasOwnProperty(key)) size++; }
        return size;
    };
    
    var count = 0,
        size=0;
    var disklocs = {}; //will store each disk loc, format: file-loc%4kb, ie file-0, file-4096, etc
    
    db.getCollection(collection).find(query).limit(limit).showDiskLoc().forEach(
        function(doc) {
            var file = doc.$diskLoc.file,
                offset = (doc.$diskLoc.offset),
                offsetPage = offset - offset % 4096;
            count++;
            size += Object.bsonsize(doc) - 45; //$diskloc info adds 45 bytes           
            disklocs[file + "-" + offsetPage] = 1;
        }
    );
    var numpages = Object.size(disklocs);
    var numbytespages = 1024 * 4 * numpages;
    print("Size of returned data in bytes: " + size);
    print("Size of pages touched by data : " + numbytespages);
    print("Compactness: " + Math.floor(100*size/numbytespages) + "%");
}
