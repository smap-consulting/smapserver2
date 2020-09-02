/*
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

*/
"use strict";

define([],
    function() {

        /*
		 * Variables for indexedDB Storage
		 */
        let databaseVersion = 1;
        let databaseName = "webform";

        var db;                     // indexedDb

        let mediaStoreName = "media";
        var mediaStore;

        let recordStoreName = "records";
        var recordStore;

        var idbSupported = typeof window.indexedDB !== 'undefined';

        return {
            isSupported: isSupported,
            init: init
        };


        /**
         * Return true if indexedDB is supported
         * No need to check for support of local storage this is checked by "store"
         * @return {Boolean}
         */
        function isSupported() {
            return idbSupported;
        };

        /**
         * Initialize indexdDb
         */
        function init() {
            return new Promise((resolve, reject) => {

                if(idbSupported) {
                    var request = window.indexedDB.open(databaseName, databaseVersion);

                    request.onerror = function (event) {
                        reject();
                    };

                    request.onsuccess = function (event) {
                        db = event.target.result;

                        db.onerror = function (event) {
                            // Generic error handler for all errors targeted at this database's
                            // requests!
                            console.error("Database error: " + event.target.errorCode);
                        };

                        resolve();
                    };

                    request.onupgradeneeded = function(event) {
                        var db = event.target.result;
                        var oldVersion = db.oldVersion || 0;

                        switch (oldVersion) {
                            case 0:
                                mediaStore = db.createObjectStore(mediaStoreName);

                                recordStore = db.createObjectStore(recordStoreName, {keyPath: 'id', autoIncrement: true});
                                recordStore.createIndex('assignment', 'assignment.assignment_id', {unique: false});
                        }
                    };

                } else {
                    reject();
                }

            });
        };


        /*
		 * Delete all media with the specified prefix
		 * Assumes db has been initialised
		 * An explicit boolean "all" is added in case the function is called accidnetially with an undefined directory
		 */
        fileStore.deleteMedia = function(dirname, all) {

            if(typeof dirname !== "undefined" || all) {

                if(dirname) {
                    console.log("delete media for: " + dirname);
                } else {
                    console.log("delete all attachments");
                }

                var prefix = dirname;

                // indexeddb first
                var objectStore = db.transaction([mediaStoreName], "readwrite").objectStore(mediaStoreName);
                objectStore.openCursor().onsuccess = function(event) {
                    var cursor = event.target.result;
                    if (cursor) {
                        if (all || cursor.key.startsWith(prefix)) {     // Don't need to check the key if all is set as everything in the data store is a media URL
                            if(cursor.value) {
                                window.URL.revokeObjectURL(cursor.value);
                            }
                            var request = objectStore.delete(cursor.key);
                            request.onsuccess = function (event) {
                                console.log("Delete: " + cursor.key);
                            };
                            cursor.continue();
                        }
                    }
                };

            }
        };

        /*
		 * Save an attachment
		 */
        fileStore.saveFile = function(media, dirname) {

            console.log("save file: " + media.name + " : " + dirname);

            var transaction = db.transaction([mediaStoreName], "readwrite");
            transaction.onerror = function(event) {
                // Don't forget to handle errors!
                alert("Error: failed to save " + media.name);
            };

            var objectStore = transaction.objectStore(mediaStoreName);
            var request = objectStore.put(media.dataUrl, dirname + "/" + media.name);

        };

        /*
		 * Get a file from idb
		 */
        fileStore.getFile = function(name, dirname) {

            return new Promise((resolve, reject) => {

                var key = dirname + "/" + name;


                getFileFromIdb(key).then(function (file) {

                    if (file) {
                        resolve(file);

                    } else {
                        reject("Error: " + err.message);
                    }

                }).catch(function (reason) {
                    reject(reason);
                });
            });

        };

        /*
		 * Obtains blob for specified file
		 */
        fileStore.retrieveFile = function(dirname, file) {

            return new Promise((resolve, reject) => {

                var updatedFile = {
                    fileName: file.fileName
                };

                fileStore.getFile(file.fileName, dirname).then(function(objectUrl){
                    updatedFile.blob = fileStore.dataURLtoBlob(objectUrl);
                    updatedFile.size = updatedFile.blob.size;
                    resolve(updatedFile);
                });


            });

        };

        // From: http://stackoverflow.com/questions/6850276/how-to-convert-dataurl-to-file-object-in-javascript
        fileStore.dataURLtoBlob = function(dataurl) {
            var arr = dataurl.split(',');
            var mime;
            var bstr;
            var n;
            var u8arr;

            if(arr.length > 1) {
                mime = arr[0].match(/:(.*?);/)[1];
                bstr = atob(arr[1]);
                n = bstr.length;
                u8arr = new Uint8Array(n);
                while (n--) {
                    u8arr[n] = bstr.charCodeAt(n);
                }
                return new Blob([u8arr], {type: mime});
            } else {
                return new Blob();
            }
        }

        /*
		 * Local functions
		 * May be called from a location that has not intialised fileStore (ie fileManager)
		 */
        function getFileFromIdb(key) {
            return new Promise((resolve, reject) => {
                if (!db) {
                    fileStore.init().then(function () {
                        resolve(completeGetFileRequest(key));
                    });
                } else {
                    resolve(completeGetFileRequest(key));
                }
            });
        }

        function completeGetFileRequest(key) {
            return new Promise((resolve, reject) => {
                var transaction = db.transaction([mediaStoreName], "readonly");
                var objectStore = transaction.objectStore(mediaStoreName);
                var request = objectStore.get(key);

                request.onerror = function(event) {
                    reject("Error getting file");
                };

                request.onsuccess = function (event) {
                    resolve(request.result);
                };
            });
        }
    });




