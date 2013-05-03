from lxml import etree
import argparse
from os import path
from bz2file import BZ2File # python 2.7 native bz2 module does not support multi stream
import iso8601
import simplejson as json

users={}

def handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

def update_counts(id, type, created):
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
    print "processing %s" % (fullhistoryfilepath,)
    cnt = 0
    with BZ2File(fullhistoryfilepath) as f:
        print "starting..."
        tree = etree.iterparse(f)
        osm = tree.next()
        for action, elem in tree:
            u = elem.get('user')
            id = elem.get('uid')
            t = elem.get('timestamp')
            v = elem.get('version')
            created = v == '1'
            type = elem.tag
            if id:
                t = iso8601.parse_date(t)
                if id not in users:
                    nodes = {'created': 0, 'modified': 0, 'deleted': 0}
                    ways = {'created': 0, 'modified': 0, 'deleted': 0}
                    relations = {'created': 0, 'modified': 0, 'deleted': 0}
                    users[id] = {'first': t, 'last': t, 'name': u, 'nodes': nodes, 'ways': ways, 'relations': relations}
                    print 'added new user %s' % (u, )
                else:
                    # update user
                    uref = users[id]
                    users[id]['first'] = min(uref['first'], t)
                    users[id]['last'] = max(uref['last'], t)
#                    print 'update existing user %s' % (u, )
                update_counts(id, type, created)
#               print "%s %s by %s" % (type, "created" if created else "modified", u)
            elem.clear()
    print "number of users: %i" % (len(users), )
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('fullhistoryfilepath', help='Path to the bzip2ed OSM full history file')
    args = parser.parse_args()
    if path.exists(args.fullhistoryfilepath):
        process_history(args.fullhistoryfilepath)
        print "finishing, dumping output as JSON"
        fname = path.join(path.dirname(args.fullhistoryfilepath), path.basename(args.fullhistoryfilepath).split('.')[0] + '.json')
        with open(fname, 'w') as outfile:
            outfile.write(json.dumps(users, default=handler))
    else:
        print "%s does not exist, check path" % (args.fullhistoryfilepath, )
        exit(1)
