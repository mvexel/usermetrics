#!/usr/bin/python

from os import path, listdir
import argparse
from subprocess import call
from threading import Thread, active_count
from Queue import Queue
from time import sleep
from uuid import uuid4

maxtasks = 16 # the number of polys to parse per thread
maxthreads = 4 # the number of osm-history-splitter maxthreads
tmpdir = '/tmp' # only used for osm-history-splitter config files
osmhistorysplittercmd = 'osm-history-splitter' # path to osm-history-splitter executable
simulate = True

def split_chunk(infilename, files):
    '''Accepts an input history file and a list of poly files, creates a splitter config file and fires the splitter'''
    print "starting splitter thread"
    print files
    with open(path.join(tmpdir, 'batch_' + str(uuid4()) + '.conf'), 'wb') as outfile:
        for f in files:
            name = path.splitext(path.split(f)[1])[0]
            outfile.write('%s.osh.bz2\tPOLY\t%s\n' % (path.join(outdir,name), path.join(polydir,f)))
    if not simulate:
        call([osmhistorysplittercmd, '--hardcut', infilename, path.join(tmpdir, 'batch.conf')])
    else:
        sleep(2)
    print "done" 
	 
def process_files(infilename):
    global polydir
    print "doing %s from %s" % (polydir,infilename)
    files = [f for f in listdir(polydir) if path.isfile(path.join(polydir,f)) and f.endswith(".poly")]
    dirs = [d for d in listdir(polydir) if path.isdir(path.join(polydir,d))]
    if len(files) == 0:
        print "this directory does not contain any .poly files."
        exit(1)
    while len(files) > 0:
        while active_count() < maxthreads:
            print "spawning thread..."
            t = Thread(target=split_chunk, args = (infilename, files[:maxtasks]))
            #t.daemon = True
            t.start()
            del files[:maxtasks]
            print "%i threads running" % (active_count())
    while len(dirs) > 0:
        nextdir = dirs.pop()
        polydir = path.join(polydir, nextdir)
        nextinfile = path.join(outdir, nextdir) + ".osh.bz2"
        process_files(nextinfile)

if __name__ == "__main__":
    polydir = None
    outdir = None
    q = Queue()
    parser = argparse.ArgumentParser()
    parser.add_argument("polydir", help="The directory of POLY files to process")
    parser.add_argument("oshfilename", help="The OSM history file to process")
    parser.add_argument("outdir", help="The directory to write output to")
    parser.add_argument("-t", help="Number of concurrent tasks to feed to splitter (default %i)" % (maxtasks,))
    parser.add_argument("-T", help="Number of concurrent splitter threads to fire (default %i)" % (maxthreads,))
    args = parser.parse_args()
    if not path.isfile(args.oshfilename):
        print "there is no file %s" % (args.oshfilename,)
        exit(1)
    if not path.isdir(args.polydir):
        print "there is no directory %s" % (args.polydir)
	exit(1)
    else:
        polydir = args.polydir
    if not path.isdir(args.outdir):
        print "there is no directory %s" % (args.outdir)
        exit(1)
    else:
        outdir = args.outdir
    if args.t:
        maxtasks = int(args.t)
    if args.T:
        maxthreads = int(args.T)
    process_files(args.oshfilename)
