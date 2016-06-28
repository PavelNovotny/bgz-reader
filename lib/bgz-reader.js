/**
 *
 * Created by pavelnovotny on 02.10.15.
 */
var NodeCache = require( "node-cache" );
var fdCache = require( "fd-cache" );
var objectCache = require( "object-cache" );
var zlib = require('zlib');
var fs = require('fs');
var bunyan = require('bunyan');
var async = require('async');
var log = bunyan.createLogger({name: "bgzReader"});
log.level("info");

var unzippedCache = new NodeCache({ stdTTL: 180, checkperiod: 200 });

fdCache.init({ttl:30000, checkperiod:10000});
objectCache.init({ttl:30000, checkperiod:5000});

exports.readInt = function readInt(bgz, offset, len, callback) {
    read(bgz, offset, len, function(err, buffer) {
        if (err) return callback(err);
        return callback(null, buffer.readIntBE(0,len));
    });
}

exports.readString = function readString(bgz, offset, len, callback) {
    read(bgz, offset, len, function(err, buffer) {
        if (err) return callback(err);
        return callback(null, buffer.toString("utf8",0,len));
    });
}

function read(bgz, offset, len, callback) {
    var bgzIndexFd;
    var end = offset + len;
    async.waterfall([
        function(callback) {
            fdCache.getFd(bgz.bgzIndexFile, callback);
        },
        function (fd, callback) {
            bgzIndexFd = fd;
            readBufCount(bgzIndexFd, callback);
        },
        function (bufCount, callback) {
            objectCache.getObject("indexBufs@"+bgz.bgzIndexFile, readIndexBuffers,{fd:bgzIndexFd, bufCount:bufCount}, null, callback);
        },
        function (indexBufs, callback) {
            var affectedBuffs = findIndexBuffers(indexBufs, offset, end);
            impactedBlocks(bgzIndexFd, offset, end, affectedBuffs.positionStart, affectedBuffs.rowCount, callback);
        },
        function (impactedBlocks, callback) {
            //todo podívat se, jestli by nestálo za to refactorovat.
            unzipChunks(bgz.bgzFile, impactedBlocks, offset, len, callback);
        }
    ], function (err, buffer) {
        if (err) return callback(err);
        return callback(null, buffer);
    });
}
exports.readBufCount = readBufCount;
function readBufCount(fd, callback) {
    fs.read(fd, new Buffer(4), 0, 4, 0, function(err, bytes, fileBuf) {
        if (err) return callback(err);
        var bufCount = fileBuf.readIntBE(0, 4);
        callback(null, bufCount);
    });
}

exports.readIndexBuffers = readIndexBuffers;
function readIndexBuffers(params, callback) {
    var bufSize = (28*params.bufCount);
    var indexBufs =  [];
    fs.read(params.fd, new Buffer(bufSize), 0, bufSize, 4, function(err, bytes, fileBuf) {
        if (err)  return callback(err);
        for (var bufNo = 0; bufNo < params.bufCount; bufNo++) {
            indexBufs.push({
                gzipAddr : fileBuf.readIntBE(0, 8),
                realAddr : fileBuf.readIntBE(8, 8),
                positionStart : fileBuf.readIntBE(16, 8),
                rowCount : fileBuf.readIntBE(24, 4)
            });
            fileBuf = fileBuf.slice(28);
        }
        return callback(null, indexBufs);
    });
}

exports.findIndexBuffers = findIndexBuffers;
//tato funkce může být synchronní, není žádný benefit mít to asynchronně.
function findIndexBuffers(indexBufs, unzippedStart, unzippedEnd) {
    var indexStart = indexBufs.length-1;
    var rowCount = 0;
    for (var bufNo = 0; bufNo < indexBufs.length; bufNo++) {
        if (indexBufs[bufNo].realAddr > unzippedStart) { //právě jsme to překročili
            indexStart = bufNo-1;
            for (var bufNo = indexStart; bufNo < indexBufs.length; bufNo++) { //s endem začneme tam, kde jsme skončili se startem.
                if (indexBufs[bufNo].realAddr > unzippedEnd) {
                    break;
                }
                rowCount += indexBufs[bufNo].rowCount;
            }
            break;
        }
    }
    if(indexStart === (indexBufs.length-1)) {
        rowCount += indexBufs[indexStart].rowCount;
    }
    return {positionStart: indexBufs[indexStart].positionStart, rowCount: rowCount};
}

exports.impactedBlocks = impactedBlocks;
function impactedBlocks(fd, unzipStart, unzipEnd, filePos, rowCount, callback) {
    var expectedBufSize = (rowCount*16) + 16; //plus 16, pro čtení další hodnoty realAddr (realAddrTo)
    var impactedBlocks = [];
    fs.read(fd, new Buffer(expectedBufSize), 0, expectedBufSize, filePos, function(err, bytes, buffer) {
        if (err)  return callback(err);
        var lastRealAddrTo;
        if (expectedBufSize > bytes) { //načetli jsme konec souboru, je potřeba doplnit jako poslední adresu velikost souboru.
            buffer = Buffer.concat([buffer, new Buffer(expectedBufSize-buffer.length)], expectedBufSize); //doplníme velikost bufferu na expected, pokud chybí.
            lastRealAddrTo = buffer.readIntBE(buffer.length - 16, 8);
            var gzipAddr = buffer.readIntBE(buffer.length - 32, 8);
            buffer.writeIntBE(lastRealAddrTo,buffer.length-8,8); //dostaneme to na pozici kde to čekáme, tj. o 8 bytů posunout než byl původní údaj o délce souboru.
            buffer.writeIntBE(gzipAddr+500000,buffer.length-16,8); //místo real délky souboru tady má být gzipTo. To ale neznáme, přidáme 0,5MB (max. odhad)
        }
        for (var blockNo = 0; blockNo < rowCount; blockNo++) {
            var realAddrFrom = buffer.readIntBE(8, 8);
            var realAddrTo = buffer.readIntBE(24, 8);
            if (realAddrFrom <= unzipEnd && unzipStart <= realAddrTo) { //intervaly se překrývají
                impactedBlocks.push({realAddrFrom: realAddrFrom, realAddrTo: realAddrTo, gzipAddrFrom: buffer.readIntBE(0, 8), gzipAddrTo: buffer.readIntBE(16, 8)});
            } else if (impactedBlocks.length > 0 ) {
                break;
            }
            buffer = buffer.slice(16);
        }
        log.info("impactedBlocks:", impactedBlocks);
        return callback(null, impactedBlocks);
    });
}

function unzipChunk(params, callback) {
    fdCache.getFd(params.bgzFile, function(err, fd) {
        if (err) return callback(err);
        var bufferSize = params.impactedBlock.gzipAddrTo - params.impactedBlock.gzipAddrFrom;
        fs.read(fd, new Buffer(bufferSize), 0, bufferSize, params.impactedBlock.gzipAddrFrom, function(err, bytes, buffer) {
            if (err) return callback(err);
            zlib.gunzip(buffer, callback);
        });
    });
}

//todo testy na unzip
function unzipChunks(bgzFile, impactedBlocks, offset, len, callback) {
    async.each(impactedBlocks, function(impactedBlock, callback) {
        log.info("unzipChunk impactedBlock", impactedBlock);
        var key = impactedBlock.realAddrFrom + "@" + bgzFile;
        objectCache.getObject(key, unzipChunk, {bgzFile: bgzFile, impactedBlock: impactedBlock}, null, function(err, chunk) {
            if (err) return callback(err);
            impactedBlock.buffer = chunk;
            log.info("unzipped impactedBlock", impactedBlock);
            return callback(null);
        });
    }, function (err) {
        if (err) return callback(err);
        var buffer = new Buffer(0);
        for (var blockNo = 0; blockNo < impactedBlocks.length; blockNo++) {
            buffer = Buffer.concat([buffer,impactedBlocks[blockNo].buffer]);
        }
        var bufferAddrStart = offset - impactedBlocks[0].realAddrFrom;
        var bufferAddrEnd = bufferAddrStart + len;
        buffer = buffer.slice(bufferAddrStart, bufferAddrEnd);
        return callback(null, buffer);
    });
}

function readFileInt(fd, offset, len, callback) {
    fs.read(fd, new Buffer(len), offset, len, 0,function(err, bytes, buffer) {
        if (err) return callback(err);
        var fileInt = buffer.readIntBE(0, len);
        callback(null, fileInt);
    });
}



