from lxml import etree
import argparse
import sys
from os import path
from bz2file import BZ2File # python 2.7 native bz2 module does not support multi stream
import iso8601
import simplejson as json
from numpy import mean

users={}
cutoff=0

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
    

def process_history(fullhistoryfilepath):
    '''this is the main xml parsing function'''
    print "processing %s" % (fullhistoryfilepath,)
    cnt = 0
    cntn = 0
    cntw = 0
    cntr = 0
    with BZ2File(fullhistoryfilepath) as f:
        print "starting..."
        # initiate the sax parser
        tree = etree.iterparse(f)
        # get the first element
        osm = tree.next()
        # iterate over all elements
        for action, elem in tree:
            cnt += 1
            if not cnt % 1000 :
                sys.stdout.write("nodes %i ways %i relations %i total elements %i users %i\r" % (cntn, cntw, cntr, cnt, len(users)))
            if (cntn + cntw + cntr) > cutoff:
                break
            # get salient attributes
            u = elem.get('user')
            id = elem.get('uid')
            t = elem.get('timestamp')
            v = elem.get('version')
            # set boolean if the object is just created (version 1)
            created = v == '1'
            # set osm element type identified by xml tag
            type = elem.tag
            if type in ('node','way','relation'):
                if type == 'node':
                    cntn+=1
                if type == 'way':
                    cntw+=1
                if type == 'relation':
                    cntr+=1
                # parse the date into a python datetime
                t = iso8601.parse_date(t)
                # check if the user already exists in the dictionary
                if id not in users:
                    # create object dictionaries
                    nodes = {'created': 0, 'modified': 0, 'deleted': 0}
                    ways = {'created': 0, 'modified': 0, 'deleted': 0}
                    relations = {'created': 0, 'modified': 0, 'deleted': 0}
                    # add the user to the dictionary
                    users[id] = {'first': t, 'last': t, 'name': u, 'nodes': nodes, 'ways': ways, 'relations': relations}
#                   print 'added new user %s' % (u, )
                else:
                    # update existing user
                    # get user data from dictionary
                    uref = users[id]
                    #determine new min / max editing timestamp
                    users[id]['first'] = min(uref['first'], t)
                    users[id]['last'] = max(uref['last'], t)
                # update all counts
                update_counts(id, type, created)
            # clear the element object and free the memory
            elem.clear()
            del elem
            del action
    # print stat
    print "\n\n"
    return

def generate_stats():
    print "\n\nStats\n=====\n\n"
    timespans = []
    for k in users.keys():
        user = users[k]
        timespans.append((iso8601.parse_date(user['last']) - iso8601.parse_date(user['first'])).total_seconds())
    mean_time_mapped = sum(timespans) / len(timespans)
    print mean_time_mapped
    mean_days_mapped = (mean_time_mapped - (mean_time_mapped % 60*60*24)) / (60*60*24)
    print mean_days_mapped
    
if __name__ == "__main__":
    # get command line argument
    parser = argparse.ArgumentParser()
    parser.add_argument('fullhistoryfilepath', help='Path to the bzip2ed OSM full history file')
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
