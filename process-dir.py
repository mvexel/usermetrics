#!/usr/bin/python

from os import path, listdir
import argparse
from subprocess import call

threads = 8
tmpdir = '/tmp'
osmhistorysplittercmd = 'osm-history-splitter'

def process_files(dirname, infilename):
    files = [f for f in listdir(dirname) if path.isfile(path.join(dirname,f)) and f.endswith(".poly")]
    if len(files) == 0:
        print "this directory does not contain any .poly files."
        exit(1)
    print "processing %i files in chunks of %i" % (len(files), min(threads,len(files)),)
    while len(files) > 0:
        batch = files[:threads]
        files = files[threads:]
        with open(path.join(tmpdir, 'batch.conf'), 'wb') as outfile:
            for f in batch:
                name = path.splitext(path.split(f)[1])[0]
                outfile.write('%s.osh.bz2\tPOLY\t%s\n' % (name, path.join(dirname,f)))
        call([osmhistorysplittercmd, '--hardcut', infilename, path.join(tmpdir, 'batch.conf')])
    print "done" 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dirname", help="The directory of POLY files to process")
    parser.add_argument("oshfilename", help="The OSM history file to process")
    parser.add_argument("-c", help="Number of concurrent threads to feed to splitter (default %i)" % (threads,))
    args = parser.parse_args()
    if not path.isfile(args.oshfilename):
        print "there is no file %s" % (args.oshfilename,)
        exit(1)
    if not path.isdir(args.dirname):
        print "there is no directory %s" % (args.dirname)
    if args.c:
        threads = int(args.c)
    process_files(args.dirname, args.oshfilename)
