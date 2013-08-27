#
#  Copyright 2009-2013 10gen, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn';

# TODO update this
plan tests => 5;

# standalone mongod
{
    ok(!$conn->_readpref_pinned, 'nothing should be pinned yet');
    throws_ok {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY);
    } qr/Read preference must be used with a replica set/,
    'read_preference PRIMARY failure on standalone mongod';

    throws_ok {
        $conn->read_preference(MongoDB::MongoClient->SECONDARY);
    } qr/Read preference must be used with a replica set/,
    'read_preference SECONDARY failure on standalone mongod';

    ok(!$conn->_readpref_pinned, 'still nothing pinned');

    my $database = $conn->get_database('readpref');
    my $collection = $database->get_collection('standalone');
    foreach (1..20) {
        $collection->insert({'a' => $_});
    }

    # make sure we can still query
    is($collection->count(), 20, 'can count the entries');

}

END {
    if ($conn) {
        $conn->get_database('readpref')->drop;
    }
}
