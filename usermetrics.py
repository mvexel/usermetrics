from lxml import etree
import argparse
from os import path

fullhistoryfilepath = ""

def process_history(fullhistoryfilepath):
    print "processing %s" % (fullhistoryfilepath,)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('fullhistoryfilepath', help='Path to the bzip2ed OSM full history file')
    args = parser.parse_args()
    if path.exists(args.fullhistoryfilepath):
        process_history(args.fullhistoryfilepath)
    else:
        print "%s does not exist, check path" % (args.fullhistoryfilepath, )
        exit(1)
