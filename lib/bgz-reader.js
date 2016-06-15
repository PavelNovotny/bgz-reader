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
log.level("error");

var unzippedCache = new NodeCache({ stdTTL: 180, checkperiod: 200 });

fdCache.init({ttl:30000, checkperiod:10000});

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
    async.waterfall([
        function(callback) {
            fdCache.getFd(bgz.bgzIndexFile, callback);
        },
        function (fd, callback) {
            bgzIndexFd = fd;
            readFileInt(fd, 0, 4, callback); //blockBufferCount, header
        },
        function (blockBufferCount, callback) {
            readBgzIndexBlockBuffers(bgzIndexFd, offset, len, blockBufferCount, callback);
        },
        function (positionStart, rowCount, callback) {
            readBgzIndexBlocks(bgzIndexFd, offset, len, positionStart, rowCount, callback);
        },
        function (impactedBlocks, callback) {
            unzipChunks(bgz.bgzFile, impactedBlocks, offset, len, callback);
        }

    ], function (err, buffer) {
        if (err) return callback(err);
        return callback(null, buffer);
    });
}

exports.readBufCount = function readBufCount(fd, callback) {
    fs.read(fd, new Buffer(4), 0, 4, 0, function(err, bytes, fileBuf) {
        if (err) return callback(err);
        var bufCount = fileBuf.readIntBE(0, 4);
        callback(null, bufCount);
    });
}

exports.readIndexBuffers = function readIndexBuffers(fd, bufCount, callback) {
    var bufSize = (28*bufCount);
    var indexBufs =  [];
    fs.read(fd, new Buffer(bufSize), 0, bufSize, 4, function(err, bytes, fileBuf) {
        if (err)  return callback(err);
        for (var bufNo = 0; bufNo < bufCount; bufNo++) {
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

//tato funkce může být synchronní, není žádný benefit mít to asynchronně.
exports.findIndexBuffers = function findIndexBuffers(indexBufs, unzippedStart, unzippedEnd) {
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


//todo čtení ind souborů dát do modulu, včetně testů, cachování blockbuffer (jako objekty), ostatní se cachovat nemusí, nebo možná to udělat a vyzkoušet.
function readBgzIndexBlockBuffers(fdIndexFile, unzippedStart, unzippedLen, blockBufferCount, callback) {
    var bufferSize = (28*blockBufferCount)+16; //plus 16 aby to nepadalo až budeme chtít číst poslední hodnotu realAddrTo
    var rowCount = 0;
    var positionStart;
    var unzippedEnd = unzippedStart + unzippedLen;
    fs.read(fdIndexFile, new Buffer(bufferSize), 0, bufferSize, 4, function(err, bytes, buffer) {
        if (err)  return callback(err);
        buffer.writeIntBE(Number.MAX_SAFE_INTEGER, buffer.length-8, 8);
        for (var blockBufferNo = 0; blockBufferNo < blockBufferCount; blockBufferNo++) {
            var realAddrFrom = buffer.readIntBE(8, 8);
            var realAddrTo = buffer.readIntBE(36, 8);
            if (realAddrFrom <= unzippedEnd && unzippedStart <= realAddrTo) { //intervaly se překrývají
                var currRowCount = buffer.readIntBE(24, 4);
                if (positionStart === undefined) {
                    positionStart = buffer.readIntBE(16, 8);
                }
                rowCount += currRowCount;
            } else if (positionStart !== undefined) {
                break;
            }
            buffer = buffer.slice(28);
        }
        log.info("positionStart:", positionStart);
        log.info("rowCount:", rowCount);
        return callback(null, positionStart, rowCount);
    });
}

function impactedBlocks(fd, indexBufs, bufIndexes, unzippedStart, unzippedLen, callback) {
    var unzippedEnd = unzippedStart + unzippedLen;
    var bufIndexes = findIndexBuffers(fd, unzippedStart, unzippedLen);
    for (var bufNo = bufIndexes.indexStart; bufNo <= bufIndexes.indexEnd; bufNo++) {

    }
    callback(null, {indexStart: indexStart, indexEnd: indexEnd});
}

function readBgzIndexBlocks(fdIndexFile, unzippedStart, unzippedLen, positionStart, rowCount, callback) {
    var bufferSize = (rowCount*16) + 16; //plus 16, pro čtení další hodnoty realAddr (realAddrTo)
    var impactedBlocks = [];
    var unzippedEnd = unzippedStart + unzippedLen;
    fs.read(fdIndexFile, new Buffer(bufferSize), 0, bufferSize, positionStart, function(err, bytes, buffer) {
        if (err)  return callback(err);
        var lastRealAddrTo;
        if (bufferSize > buffer.length) { //jsme na konci je potřeba doplnit jako poslední adresu velikost souboru.
            lastRealAddrTo = buffer.readIntBE(buffer.length - 8, 8);
            var gzipAddr = buffer.readIntBE(buffer.length - 24, 8);
            buffer = Buffer.concat([buffer, new Buffer(8)]);
            buffer.writeIntBE(lastRealAddrTo,buffer.length-8,8); //dostaneme to na pozici kde to čekáme
            buffer.writeIntBE(gzipAddr+500000,buffer.length-16,8); //poslední gzip adresu neznáme, přidáme 0,5MB (max. odhad)
        }
        for (var blockNo = 0; blockNo < rowCount; blockNo++) {
            var realAddrFrom = buffer.readIntBE(8, 8);
            var realAddrTo = buffer.readIntBE(24, 8);
            if (realAddrFrom <= unzippedEnd && unzippedStart <= realAddrTo) { //intervaly se překrývají
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


function unzipChunks(bgzFile, impactedBlocks, offset, len, callback) {
    async.each(impactedBlocks, function(impactedBlock, callback) {
        var bufferSize = impactedBlock.gzipAddrTo - impactedBlock.gzipAddrFrom;
        log.info("unzipChunk impactedBlock", impactedBlock);
        var key = bgzFile + impactedBlock.realAddrFrom;
        var cachedBuffer = unzippedCache.get(key);
        if (cachedBuffer !== undefined) {
            impactedBlock.buffer = cachedBuffer;
            return callback(null);
        }
        fdCache.getFd(bgzFile, function(err, fd) {
            if (err) return callback(err);
            fs.read(fd, new Buffer(bufferSize), 0, bufferSize, impactedBlock.gzipAddrFrom, function(err, bytes, buffer) {
                if (err) return callback(err);
                zlib.gunzip(buffer, function(err, unzippedBuf) {
                    if (err) return callback(err);
                    impactedBlock.buffer = unzippedBuf;
                    log.info("unzipChunk impactedBlock with buffer", impactedBlock);
                    unzippedCache.set(key, unzippedBuf);
                    return callback(null);
                });
            });
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



