/**
 *
 * Created by pavelnovotny on 02.10.15.
 */

var bgzReader = require("../lib/bgz-reader.js");
var fs = require("fs");
var assert = require("assert");
var bunyan = require('bunyan');
var log = bunyan.createLogger({name: "testBgzReader"});
log.level("info");
var testIndexFile = {bgzFile:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.hash_v1.bgz", bgzIndexFile:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.hash_v1.bgz.ind"};
var testFile = {bgzFile:"../hashSeek/hashSeekFiles/jms_s1_alsb_aspect.audit.20150425.bgz", bgzIndexFile:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.ind"};

describe('bgzReader', function() {
    describe('#readInt()', function() {
        it('should read int from bgz file', function(done) {
            bgzReader.readInt(testIndexFile, 0, 4, function(int) {
                log.info("int", int);
                done();
            });
        });
    });
    describe('#readIntNext()', function() {
        it('should be much quicker', function(done) {
            bgzReader.readInt(testIndexFile, 0, 4, function(int) {
                log.info("int", int);
                done();
            });
        });
    });
    describe('#readString()', function() {
        it('should read a string', function(done) {
            bgzReader.readString(testFile, 0, 4, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read much quicker a string', function(done) {
            bgzReader.readString(testFile, 0, 100, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 958287412, 6242, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 958359464, 16549, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 958295758, 17538, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 2326806730, 7756, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 3067240835, 16967, function(string) {
                log.info("String", string);
                done();
            });
        });
    });
    describe('#readStringNext()', function() {
        it('should read from middle of the file', function(done) {
            bgzReader.readString(testFile, 5459406307, 5108, function(string) {
                log.info("String", string);
                done();
            });
        });
    });

});

describe('bgzIndexReader', function() {
    describe('#readBufCount()', function() {
        it('should read buffer count', function(done) {
            var fd = fs.openSync("/Users/pavelnovotny/binaryGrammar/other_s1_alsb_aspect.audit.20150413.bgz.ind","r");
            bgzReader.readBufCount(fd, function(err, bufCount) {
                assert.equal(bufCount, 9, "bufCount");
                done();
            });
        });
    });
    describe('#readBgzIndexBuffers()', function() {
        it('should read complete bgz index buffer', function(done) {
            var fd = fs.openSync("/Users/pavelnovotny/binaryGrammar/other_s1_alsb_aspect.audit.20150413.bgz.ind","r");
            bgzReader.readIndexBuffers(fd, 9, function(err, indexBufs) {
                assert.equal(indexBufs.length,9, "IndexBufs of tested file");
                assert.equal(indexBufs[0].gzipAddr,0, "gzipAddr of tested file");
                assert.equal(indexBufs[0].realAddr,0, "realAddr of tested file");
                assert.equal(indexBufs[0].positionStart,256, "positionStart of tested file");
                assert.equal(indexBufs[0].rowCount,300, "rowCount of tested file");
                assert.equal(indexBufs[8].gzipAddr,28682182, "gzipAddr of tested file");
                assert.equal(indexBufs[8].realAddr,479904000, "realAddr of tested file");
                assert.equal(indexBufs[8].positionStart,38656, "positionStart of tested file");
                assert.equal(indexBufs[8].rowCount,240, "rowCount of tested file");
                done();
            });
        });
    });
    describe('#findIndexBuffers()', function() {
        var indexBufs = [{"gzipAddr": 0, "realAddr": 0, "positionStart": 256, "rowCount": 301}, {
            "gzipAddr": 2256951,
            "realAddr": 59988000,
            "positionStart": 5056,
            "rowCount": 302
        }, {"gzipAddr": 4894591, "realAddr": 119976000, "positionStart": 9856, "rowCount": 303}, {
            "gzipAddr": 7667656,
            "realAddr": 179964000,
            "positionStart": 14656,
            "rowCount": 304
        }, {
            "gzipAddr": 10662170,
            "realAddr": 239952000,
            "positionStart": 19456,
            "rowCount": 305
        }, {
            "gzipAddr": 19181274,
            "realAddr": 299940000,
            "positionStart": 24256,
            "rowCount": 306
        }, {
            "gzipAddr": 22889632,
            "realAddr": 359928000,
            "positionStart": 29056,
            "rowCount": 307
        }, {
            "gzipAddr": 25965057,
            "realAddr": 419916000,
            "positionStart": 33856,
            "rowCount": 308
        }, {"gzipAddr": 28682182, "realAddr": 479904000, "positionStart": 38656, "rowCount": 309}];
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 0, 0);
            assert.equal(found.positionStart, 256, "indexStart");
            assert.equal(found.rowCount, 301, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 0, 59987999);
            assert.equal(found.positionStart, 256, "indexStart");
            assert.equal(found.rowCount, 301, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 0, 59988000);
            assert.equal(found.positionStart, 256, "indexStart");
            assert.equal(found.rowCount, 603, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 0, 479904001);
            assert.equal(found.positionStart, 256, "indexStart");
            assert.equal(found.rowCount, 2745, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 479904000, 479904001);
            assert.equal(found.positionStart, 38656, "indexStart");
            assert.equal(found.rowCount, 309, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 479903999, 479904001);
            assert.equal(found.positionStart, 33856, "indexStart");
            assert.equal(found.rowCount, 617, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 479904000, 479904001);
            assert.equal(found.positionStart, 38656, "indexStart");
            assert.equal(found.rowCount, 309, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 239952000, 359928000);
            assert.equal(found.positionStart, 19456, "indexStart");
            assert.equal(found.rowCount, 305+306+307, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 239952100, 359928000);
            assert.equal(found.positionStart, 19456, "indexStart");
            assert.equal(found.rowCount, 305+306+307, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 239952100, 359928001);
            assert.equal(found.positionStart, 19456, "indexStart");
            assert.equal(found.rowCount, 305+306+307, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 239952100, 239952101);
            assert.equal(found.positionStart, 19456, "indexStart");
            assert.equal(found.rowCount, 305, "indexEnd");
            done();
        });
        it('should find right buffers', function(done) {
            var found = bgzReader.findIndexBuffers(indexBufs, 999999998, 999999999);
            assert.equal(found.positionStart, 38656, "indexStart");
            assert.equal(found.rowCount, 309, "indexEnd");
            done();
        });
    });
});
