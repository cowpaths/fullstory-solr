/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

var MB_FACTOR = 1024*1024;

/** Grey-base bar tint: newest bucket green -> yellow -> orange -> red (oldest). */
var hslToHex = function(h, s, l) {
    h = ((h % 360) + 360) % 360;
    s = Math.max(0, Math.min(100, s)) / 100;
    l = Math.max(0, Math.min(100, l)) / 100;

    var c = (1 - Math.abs(2 * l - 1)) * s;
    var x = c * (1 - Math.abs((h / 60) % 2 - 1));
    var m = l - c / 2;
    var r = 0;
    var g = 0;
    var b = 0;

    if (h < 60) {
        r = c; g = x;
    } else if (h < 120) {
        r = x; g = c;
    } else if (h < 180) {
        g = c; b = x;
    } else if (h < 240) {
        g = x; b = c;
    } else if (h < 300) {
        r = x; b = c;
    } else {
        r = c; b = x;
    }

    var toHex = function(v) {
        var n = Math.round((v + m) * 255);
        var hex = n.toString(16);
        return hex.length === 1 ? '0' + hex : hex;
    };

    return '#' + toHex(r) + toHex(g) + toHex(b);
};

var temporalBucketBarColor = function(bucketIndex, bucketCount) {
    if (bucketCount <= 1) {
        return hslToHex(120, 24, 76);
    }
    // 0 = newest (green), 1 = oldest (red)
    var t = bucketIndex / (bucketCount - 1);
    var hue = 120 * (1 - t);
    return hslToHex(hue, 30, 74);
};

var assignTemporalBucketBarColors = function(segments) {
    var uniqueBuckets = [];
    for (var i = 0; i < segments.length; i++) {
        var bucket = segments[i].temporalBucket;
        if (bucket != null && uniqueBuckets.indexOf(bucket) === -1) {
            uniqueBuckets.push(bucket);
        }
    }
    uniqueBuckets.sort(function(a, b) { return a - b; });

    var colorByBucket = {};
    for (var j = 0; j < uniqueBuckets.length; j++) {
        colorByBucket[uniqueBuckets[j]] = temporalBucketBarColor(j, uniqueBuckets.length);
    }

    for (var k = 0; k < segments.length; k++) {
        var seg = segments[k];
        if (seg.temporalBucket != null) {
            seg.bucketBarColor = colorByBucket[seg.temporalBucket];
        }
    }
};

solrAdminApp.controller('SegmentsController', function($scope, $routeParams, $interval, Segments, Constants) {
    $scope.resetMenu("segments", Constants.IS_CORE_PAGE);

    /** Fixed decimals without locale grouping (avoids confusing commas in ms timings). */
    $scope.formatMs = function(v, fracDigits) {
        if (v == null || v === '') {
            return '';
        }
        var n = Number(v);
        if (!isFinite(n)) {
            return '';
        }
        var d = fracDigits != null ? fracDigits : 4;
        return n.toFixed(d);
    };

    $scope.refresh = function() {

        Segments.get({core: $routeParams.core}, function(data) {
            var segments = data.segments;

            var segmentSizeInBytesMax = getLargestSegmentSize(segments);
            $scope.segmentMB = Math.floor(segmentSizeInBytesMax / MB_FACTOR);
            $scope.xaxis = calculateXAxis(segmentSizeInBytesMax);

            $scope.documentCount = 0;
            $scope.deletionCount = 0;

            $scope.segments = [];
            for (var name in segments) {
                var segment = segments[name];

                var segmentSizeInBytesLog = Math.log(segment.sizeInBytes);
                var segmentSizeInBytesMaxLog = Math.log(segmentSizeInBytesMax);

                segment.totalSize = Math.floor((segmentSizeInBytesLog / segmentSizeInBytesMaxLog ) * 100);

                segment.deletedDocSize = Math.floor((segment.delCount / segment.size) * segment.totalSize);
                if (segment.delDocSize <= 0.001) delete segment.deletedDocSize;

                segment.aliveDocSize = segment.totalSize - segment.deletedDocSize;

                $scope.segments.push(segment);

                $scope.documentCount += segment.size;
                $scope.deletionCount += segment.delCount;
            }
            assignTemporalBucketBarColors($scope.segments);
            $scope.deletionsPercentage = calculateDeletionsPercentage($scope.documentCount, $scope.deletionCount);
        });
    };

    $scope.toggleAutoRefresh = function() {
        $scope.autorefresh = !$scope.autorefresh;
        if ($scope.autorefresh) {
            $scope.interval = $interval($scope.refresh, 1000);
            var onRouteChangeOff = $scope.$on('$routeChangeStart', function() {
              $interval.cancel($scope.interval);
              onRouteChangeOff();
            });

        } else if ($scope.interval) {
            $interval.cancel($scope.interval);
        }
    };
    $scope.refresh();
});

var calculateXAxis = function(segmentInBytesMax) {
    var steps = [];
    var log = Math.log(segmentInBytesMax);

    for (var j=0, step=log/4; j<3; j++, step+=log/4) {
        steps.push({pos:j, value:Math.floor((Math.pow(Math.E, step))/MB_FACTOR)})
    }
    return steps;
};

var getLargestSegmentSize = function(segments) {
    var max = 0;
    for (var name in segments) {
        max = Math.max(max, segments[name].sizeInBytes);
    }
    return max;
};

var calculateDeletionsPercentage = function(docCount, delCount) {
    if (docCount == 0) {
        return 0;
    } else {
        var percent = delCount / docCount * 100;
        return Math.round(percent * 100) / 100;
    }
};
