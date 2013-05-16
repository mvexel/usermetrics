#!/usr/bin/python

from os import path, listdir
import argparse
from subprocess import call

threads = 8
tmpdir = '/tmp'
osmhistorysplittercmd = 'osm-history-splitter'

def process_files(indir, outdir, infilename):
    files = [f for f in listdir(indir) if path.isfile(path.join(indir,f)) and f.endswith(".poly")]
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
                outfile.write('%s.osh.bz2\tPOLY\t%s\n' % (os.path.join(outdir,name), path.join(indir,f)))
        call([osmhistorysplittercmd, '--hardcut', infilename, path.join(tmpdir, 'batch.conf')])
    print "done" 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("polydir", help="The directory of POLY files to process")
    parser.add_argument("oshfilename", help="The OSM history file to process")
    parser.add_argument("outdir", help="The directory to write output to")
    parser.add_argument("-c", help="Number of concurrent threads to feed to splitter (default %i)" % (threads,))
    args = parser.parse_args()
    if not path.isfile(args.oshfilename):
        print "there is no file %s" % (args.oshfilename,)
        exit(1)
    if not path.isdir(args.polydir):
        print "there is no directory %s" % (args.polydir)
    if not path.isdir(args.outdir):
        print "there is no directory %s" % (args.outdir)
    if args.c:
        threads = int(args.c)
    process_files(args.polydir, args.outdir, args.oshfilename)
