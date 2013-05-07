from lxml import etree
import argparse
import sys
from os import path
from bz2file import BZ2File # python 2.7 native bz2 module does not support multi stream
import iso8601
import simplejson as json
from numpy import mean
from datetime import datetime, timedelta
import pytz
#from guppy import hpy

users={}
cutoff=0
utc=pytz.UTC
longer_ago = 'longer ago'

def handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

def update_counts(id, type, created):
    '''this function updates the counts for the user identified
    by id for the current object that we are processing'''
    if type == 'node':
        if created:
            users[id]['nodes']['created'] += 1
        else:
            users[id]['nodes']['modified'] += 1
    elif type == 'way':
        if created:
            users[id]['ways']['created'] += 1
        else:
            users[id]['ways']['modified'] += 1
    elif type == 'relation':
        if created:
            users[id]['relations']['created'] += 1
        else:
            users[id]['relations']['modified'] += 1
    id = None
    type = None
    created = None

def process_history(fullhistoryfilepath):
    '''this is the main xml parsing function'''
    print "processing %s" % (fullhistoryfilepath,)
    cnt = 0
    cntn = 0
    cntw = 0
    cntr = 0
    if fullhistoryfilepath[-3:].lower() in ('osm','osh'):
        f = open(fullhistoryfilepath, 'rb')
    elif fullhistoryfilepath[-7:].lower() in ('osm.bz2', 'osh.bz2'):
        f = BZ2File(fullhistoryfilepath, 'rb')
    else:
        print "File needs to be .osm, .osh, .osm.bz2 or .osh.bz2"
        exit(1)
    with f:
        print "starting..."
        # iterate over all elements
        context = etree.iterparse(f)
        for action, elem in context:
            if elem is None:
                break
            cnt += 1
            if not cnt % 1000 :
                sys.stdout.write("nodes %i ways %i relations %i total elements %i users %i\r" % (cntn, cntw, cntr, cnt, len(users)))
#            if not cnt % 1.0e6:
#                h = hpy()
#                print h.heap()
            if not cutoff == 0 and (cntn + cntw + cntr) > cutoff:
                break
            # set osm element type identified by xml tag
            type = elem.tag
            if type in ('node','way','relation'):
                if type == 'node':
                    cntn+=1
                if type == 'way':
                    cntw+=1
                if type == 'relation':
                    cntr+=1
                # get salient attributes
                u = elem.get('user')
                id = elem.get('uid')
                t = elem.get('timestamp')
                v = elem.get('version')
                # parse the date into a python datetime
                t = iso8601.parse_date(t)
                # check if the user already exists in the dictionary
                # set boolean if the object is just created (version 1)
                created = v == '1'
                if id not in users:
                    # create object dictionaries
                    nodes = {'created': 0, 'modified': 0, 'deleted': 0}
                    ways = {'created': 0, 'modified': 0, 'deleted': 0}
                    relations = {'created': 0, 'modified': 0, 'deleted': 0}
                    # add the user to the dictionary
                    users[id] = {'first': t, 'last': t, 'name': u, 'nodes': nodes, 'ways': ways, 'relations': relations}
#                   print 'added new user %s' % (u, )
                    nodes = None
                    ways = None
                    relations = None
                else:
                    # update existing user
                    # get user data from dictionary
                    uref = users[id]
                    #determine new min / max editing timestamp
                    users[id]['first'] = min(uref['first'], t)
                    users[id]['last'] = max(uref['last'], t)
                    uref = None
                # update all counts
                update_counts(id, type, created)
                u = None
                id = None
                t = None
                v = None
                t = None
                created = None
            # clear the element object and free the memory
            elem.clear()
            type = None
            action = None
    # print stat
    print "\n"
    return

def generate_stats():
    print "\nStats\n=====\n"
    timespans = []
    thresholds = [30, 180, 365]
    lastmapped = {}
    for k in users.keys():
        user = users[k]
        timespans.append((user['last'] - user['first']).total_seconds())
        # mapped in last 30, 180, 360 days?
        timesincelastmapped = utc.localize(datetime.now()) - user['last']
        hasmappedrecently = False
        for threshold in thresholds:
            if timesincelastmapped < timedelta(threshold):
                hasmappedrecently = True
                if threshold in lastmapped.keys():
                    lastmapped[threshold] += 1
                else:
                    lastmapped[threshold] = 1
                break
        if not hasmappedrecently:
            if longer_ago in lastmapped.keys():
                lastmapped[longer_ago] += 1
            else:
                lastmapped[longer_ago] = 1
    for k in lastmapped.keys():
        if k != longer_ago:
            print "%i mappers have mapped in the last %i days" % (lastmapped[k], k,)
        else:
            print "%i mappers have not mapped in the last %i days" % (lastmapped[k], max(thresholds),)
    mean_time_mapped = sum(timespans) / len(timespans)
    print mean_time_mapped
    mean_days_mapped = (mean_time_mapped - (mean_time_mapped % 60*60*24)) / (60*60*24)
    print mean_days_mapped
    
if __name__ == "__main__":
    # get command line argument
    parser = argparse.ArgumentParser()
    parser.add_argument('fullhistoryfilepath', help='Path to the (bzip2ed) OSM full history file')
    parser.add_argument('--stats', help='Output basic stats afterwards', action='store_true')
    parser.add_argument('--scaffold', help='Use scaffold data', action='store_true')
    parser.add_argument('--cutoff', help='Process only up to this amount of objects')
    args = parser.parse_args()
    if args.scaffold:
        jsondata = open('scaffold.json', 'rb')
        if not jsondata:
            exit(1)
        users = json.load(jsondata)
    else:
        if args.cutoff:
            cutoff = int(args.cutoff)
        # check if the file even exists
        if path.exists(args.fullhistoryfilepath):
            # call the parser
            process_history(args.fullhistoryfilepath)
            # final output to file and command line
            fname = path.join(path.dirname(args.fullhistoryfilepath), path.basename(args.fullhistoryfilepath).split('.')[0] + '.json')
            print "Dumping output as JSON to %s" % (fname, )
            with open(fname, 'w') as outfile:
                outfile.write(json.dumps(users, default=handler))
        else:
            print "%s does not exist, check path" % (args.fullhistoryfilepath, )
            exit(1)
    if args.stats:
        generate_stats()
