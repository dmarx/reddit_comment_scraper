from urllib2 import Request, urlopen
from time import sleep
import json
import argparse
import csv
from DBhandler import *

### Global Variables ###
_headers = {"User-agent" : "reddit_comment_fetcher by /u/shaggorama"} # not sure if I need this
_host = 'http://www.reddit.com'

def get_comments(URL,head,delay=2):
    '''Pretty generic call to urllib2.'''
    sleep(delay) # ensure we don't GET too frequently or the API will block us
    request = Request(URL, headers=head)
    try:
        response = urlopen(request)
        data = response.read()
    except:
        sleep(delay+5)
        response = urlopen(request)
        data = response.read()
    return data
   
def parse_json(json_data):
    '''Simple parser for getting reddit comments from JSON output. Returns tuple of JSON comments and list of IDs'''
    try:
        decoded = json.JSONDecoder().decode(json_data)
        comments = [x['data'] for x in decoded['data']['children'] ]
        ids = [comments[i]['name'] for i in range(len(comments))]
    except:
        return [], []
    return comments, ids
   
# Probably should store comments as we get them. For now, let's just download them all and dump them into the DB all at once.   
def scrape(targetUser, limit=0, pause=2): #, db='redditComments.db', storage='db'):    
    items = []
    userpageURL = _host + '/user/' + targetUser + '/comments/.json'       
    nPages=0 
    stop = False
#    tbl_comments = ConnectToDBCommentsTable(db)
    while not stop: # or len(items)<=50: # Need to figure out how to get past the 999 comments limit.
        if nPages > 0:
            last = items[-1]['name']
            paginationSuffix = '?count='+str(nPages*25)+'&after='+last #+'.json'
            print userpageURL + paginationSuffix    
        try:
            data = get_comments(userpageURL + paginationSuffix, _headers, pause)
        except:
            data = get_comments(userpageURL, _headers, pause)
        newItems, newIDs = parse_json(data)       
        if newItems == []:
            print "Reached limit of available comments"
            stop = True # this is redundant
            break
        try:
            for i, id in enumerate(newIDs):
                if id in item_ids:
                    print "Page is returning comments we've already seen. Ending scrape."
                    stop = True
                    break
                else:
                    items.append(newItems[i])
                    item_ids.append(id)
        except:
            items, item_ids = newItems, newIDs  
        nPages += 1
        print "Downloaded %d pages, %d comments. Oldest comment: %d \n" % (nPages, len(items), items[-1]['created'])  
        if limit > 0 and len(item_ids) >= limit:
            print "Exceeded download limit set by paramter 'limit'."
            items = items[0:limit]
            stop = True #redundant with 'break' command
            break        
    return items             

def saveCommentsCSV(data, filename='redditComments.txt'):
    '''Store comments. At present, writes to CSV file. Would be better if we wrote to a DB'''
    print "Saving comments to CSV"
    writer = csv.DictWriter(open(filename, 'ab')
                      #,fieldnames=itemDict[itemDict.keys()[0]].keys()
                      ,fieldnames=data[0].keys()
                      ,delimiter=','
                      ,quotechar='|'
                      ,quoting=csv.QUOTE_MINIMAL)
    headers =  dict((k, k.encode('utf-8') if isinstance(k, unicode) else k) for k, v in data[0].iteritems())                     
    writer.writerow(headers)
    for comment in data:
        #writer.writerow({k:v.encode('utf8') for k,v in comment.items()})
        writer.writerow(dict((k, v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in comment.iteritems()))





if __name__ == '__main__':
    ### Commandline argument handling ###
    # Moved this from below imports to MAIN test to allow importation of this module. Otherwise, throws error on null required fields (i.e. USER).
    parser = argparse.ArgumentParser(description="Scrapes comments for a reddit user. Currently limited to most recent 999 comments (limit imposed by reddit).")
    parser.add_argument('-u','--user', type=str, help="Reddit username to grab comments from.", required=True)
    parser.add_argument('-l','--limit', type=int, help="Maximum number of comments to download.", default=0)
    parser.add_argument('-d','--dbname', type=str, help="Database name for storage.", default='RedditComments.DB')
    parser.add_argument('-w','--wait', type=int,help="Wait time between GET requests. Reddit documentation requests a limit of 1 of every 2 seconds not to exceed 30 per min.", default=2)
    parser.add_argument('-c','--csv', type=str, help="CSV name if you want CSV output instead of database.", default=None)

    args = parser.parse_args()
    _user   = args.user
    _limit  = args.limit
    _dbname = args.dbname
    _wait   = args.wait  
    _csv    = args.csv

    comments = scrape(_user, _limit, _wait)
    print "Total Comments Downloaded:", len(comments)
    # to pretty print results:
    #for i in comments: print json.dumps(i, indent=2)
    
    if _csv is not None:
        saveCommentsCSV(comments, _csv)        
    else:
        commentsTable = ConnectToDBCommentsTable(_dbname)
        writeToDb(commentsTable, comments)
