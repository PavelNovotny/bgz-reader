/**
 *
 * Created by pavelnovotny on 02.10.15.
 */

var bgzReader = require("../lib/bgz-reader.js");
var bunyan = require('bunyan');
var log = bunyan.createLogger({name: "testBgzReader"});
log.level("info");
var testIndexFile = {file:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.hash_v1.bgz", indexFile:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.hash_v1.bgz.ind"};
var testFile = {file:"../hashSeek/hashSeekFiles/jms_s1_alsb_aspect.audit.20150425.bgz", indexFile:"../hashSeek/hashSeekFiles/hash/jms_s1_alsb_aspect.audit.20150425.bgz.ind"};

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

