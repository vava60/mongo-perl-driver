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

plan tests => 17;

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

    my $database = $conn->get_database('test_database');
    my $collection = $database->get_collection('standalone');
    foreach (1..20) {
        $collection->insert({'a' => $_});
    }

    # make sure we can still query
    is($collection->count(), 20, 'can count the entries');
}

# three-node replica set
SKIP: {
    skip 'requires running replica set', 12 unless exists $ENV{MONGO_READPREF};
    
    my $rsconn = MongoDB::MongoClient->new(host => 'localhost', port => 27020, find_master => 1);

    # set up replica set tags
    my $replcoll = $rsconn->get_database('local')->get_collection('system.replset');
    my $rsconf = $replcoll->find_one();

    ($rsconf->{'version'})++;
    $rsconf->{'members'}->[0]{'tags'} = {disk => 'ssd', use => 'production'};
    $rsconf->{'members'}->[1]{'tags'} = {disk => 'ssd', use => 'production', rack => 'k'};
    $rsconf->{'members'}->[2]{'tags'} = {disk => 'spinning', use => 'reporting', mem => '32'};

    $rsconn->get_database('admin')->run_command({'replSetReconfig' => $rsconf});

    $rsconf = $replcoll->find_one();
    is($rsconf->{'members'}->[0]->{'tags'}->{'disk'}, 'ssd', 'check that the config is there');
    is($rsconf->{'members'}->[2]->{'tags'}->{'use'}, 'reporting', 'check config again');

    # check pinning primary with readpref PRIMARY
    $rsconn->read_preference(MongoDB::MongoClient->PRIMARY);
    is($rsconn->_master, $rsconn->_readpref_pinned, 'primary is pinned');
    
    my $collection = $rsconn->get_database('test_database')->get_collection('test_collection');
    my $cursor = $collection->find();
    is($cursor->_client->host, $rsconn->_master->host, 'cursor connects to primary');

    # check pinning primary with readpref PRIMARY_PREFERRED
    $rsconn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED);
    is($rsconn->_master, $rsconn->_readpref_pinned, 'primary is pinned');
    
    $collection = $rsconn->get_database('test_database')->get_collection('test_collection');
    $cursor = $collection->find();
    is($cursor->_client->host, $rsconn->_master->host, 'cursor connects to primary');

    # check pinning secondary with readpref SECONDARY
    $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
    my $pinhost = $rsconn->_readpref_pinned->host;
    ok($pinhost && $pinhost ne $rsconn->_master->host, 'secondary is pinned');

    # check pinning secondary with readpref SECONDARY_PREFERRED
    $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
    $pinhost = $rsconn->_readpref_pinned->host;
    ok($pinhost && $pinhost ne $rsconn->_master->host, 'secondary is pinned');

    # error cases
    throws_ok {
        $rsconn->read_preference(MongoDB::MongoClient->PRIMARY, [{use => 'production'}]);
    } qr/PRIMARY cannot be combined with tags/,
    'PRIMARY cannot be combined with tags';

    throws_ok {
        $rsconn->read_preference();
    } qr/Missing read preference mode/,
    'Missing read preference mode';

    throws_ok {
        $rsconn->read_preference(-1);
    } qr/Unrecognized read preference mode/,
    'bad readpref mode 1';

    throws_ok {
        $rsconn->read_preference(5);
    } qr/Unrecognized read preference mode/,
    'bad readpref mode 2';
}

