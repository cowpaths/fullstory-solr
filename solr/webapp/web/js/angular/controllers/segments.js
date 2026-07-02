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

/** Blue-grey (future) then green (newest) -> red (oldest); extra buckets reuse the last color. */
var BUCKET_BAR_COLORS = [
    '#a9c3d1', // -9d / far future
    '#a9d1a9', // 3d / newest typical bucket
    '#c3d1a9',
    '#d1d1a9',
    '#d1c3a9',
    '#d1b6a9',
    '#d1a9a9'
];

var buildColorByBucket = function(bucketBoundaries) {
    var colorByBucket = {};
    if (!bucketBoundaries) {
        return colorByBucket;
    }
    var lastIdx = BUCKET_BAR_COLORS.length - 1;
    for (var i = 0; i < bucketBoundaries.length; i++) {
        colorByBucket[String(bucketBoundaries[i])] = BUCKET_BAR_COLORS[Math.min(i, lastIdx)];
    }
    return colorByBucket;
};

var colorByBucketCache = { key: null, map: null };

var getColorByBucket = function(bucketBoundaries) {
    var key = bucketBoundaries ? bucketBoundaries.join('\0') : '';
    if (colorByBucketCache.key !== key) {
        colorByBucketCache.key = key;
        colorByBucketCache.map = buildColorByBucket(bucketBoundaries);
    }
    return colorByBucketCache.map;
};

var assignBucketBarColors = function(segments, colorByBucket) {
    for (var i = 0; i < segments.length; i++) {
        var bucket = segments[i].temporalBucket;
        if (bucket != null) {
            segments[i].bucketBarColor = colorByBucket[String(bucket)];
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
            var colorByBucket = getColorByBucket(data.bucketBoundaries);
            assignBucketBarColors($scope.segments, colorByBucket);
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
