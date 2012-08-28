from sqlalchemy import *

def ConnectToDBCommentsTable(dbname):
    db = create_engine('sqlite:///'+dbname)
    db.echo = False
    metadata = MetaData()
    metadata.bind = db

    comments = Table('comments', metadata
                ,Column('subreddit_id', String(10))
                ,Column('edited', Integer)
                ,Column('banned_by', String(20))
                ,Column('link_id', String(10))
                ,Column('likes', Integer)
                ,Column('replies', Integer)
                ,Column('id', String(10), primary_key=True)
                ,Column('author', String(20))
                ,Column('parent_id', String(10))
                ,Column('approved_by', String(20))
                ,Column('body', String(10000))
                ,Column('link_title', String(300))
                ,Column('author_flair_css_class', String(255))
                ,Column('downs', Integer)
                ,Column('body_html', String(10000))
                ,Column('subreddit', String(21))
                ,Column('name', String(10))
                ,Column('created', Integer)
                ,Column('author_flair_text', String(255))
                ,Column('created_utc', Integer)
                ,Column('num_reports', Integer)
                ,Column('ups', Integer)
                )
    try:                
        comments.create()
        print 'Created Database table "comments" in', dbname
    except: # exc.OperationalError:
        pass
    #    print "Table already exists."
    return comments
    
def writeToDb(table, data):
    if data <> []:    
        m=0
        for c in data:
            try:        
                ins = table.insert()
                ins.execute(c)
                m+=1           
            except: # exc.IntegrityError:
                #print sys.exc_info()[0]
                print "Comment already stored."
        print m, "new comments stored."
    else:
        print "Null data downloaded."              
    
