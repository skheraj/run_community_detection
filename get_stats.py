#!usr/bin/env python

import re, mysql.connector, dbm, csv

CNX = None

PRIMARY_PROJECT = (
    "SELECT p.name, p.category, COUNT(*) "
    "FROM project p, commit c, modified_file mf "
    "WHERE p.name=c.project AND "
    "c.commit_id=mf.commit_id AND " 
    "c.username='%s' "
    "GROUP BY p.name "
    "ORDER BY COUNT(*) DESC "
    "LIMIT 1"
)

PRIMARY_FILE = (
    "SELECT mf.name, COUNT(*) "
    "FROM project p, commit c, modified_file mf "
    "WHERE p.name=c.project AND "
    "c.commit_id=mf.commit_id AND " 
    "c.username='%s' AND "
    "mf.name NOT LIKE '%%CHANGES%%' "
    "GROUP BY mf.name "
    "ORDER BY COUNT(*) DESC "
    "LIMIT 1"
)

COMMIT_COMMENT_WORD = (
    "SELECT comment "
    "FROM commit "
    "WHERE username='%s'"
)

JIRA_DESCRIPTION_WORD = (
    "SELECT distinct ji.description "
    "FROM jira_issue ji, jira_comment jc "
    "WHERE ji.key=jc.issue_key AND "
    "jc.author_username='%s'"
)

def open_connection():
    global CNX
    print("Opening connection to database.")
    db = dbm.open('db_credentials', 'c')
    
    # check for db credentials
    if not 'user' in db or not 'password' in db or not 'database' in db:
        return False
    
    USER = db['user'].decode("utf-8")
    PASSWORD = db['password'].decode("utf-8")
    DATABASE = db['database'].decode("utf-8")
    
    CNX = mysql.connector.connect(user=USER, password=PASSWORD, database=DATABASE)
    
    return True

def close_connection():
    global CNX
    print("Closing connection to database.")
    CNX.commit()
    CNX.close
    CNX = None

def find_most_common_word(text, username, project):
    common_words = open('common_words.txt').read().split()
    words = text.split()
    words = [w for w in words if len(w) > 2 and 
                                 w.lower() not in common_words and 
                                 w.lower() not in username.lower() and 
                                 w.lower() not in project.lower()]
    most_common_word = max(set(words), key=words.count)
    most_common_word = re.sub("[^a-zA-Z]+", "", most_common_word)
    
    return most_common_word

def main():
    open_connection()
    
    output_file = open("community_stats.txt", "w")
    
    clusters = open('community_data_output.txt').read().split('\n\n')
    
    for c in clusters:
        cursor = CNX.cursor()
        users = c.split()
        cluster_stats = []
        
        for u in users:
            user_stats = {}
            user_stats['username'] = u
            
            # retrieve primary project
            print("Retrieving primary project for %s" % u)
            cursor.execute(PRIMARY_PROJECT % (u))
            for (name, category, count) in cursor:
                user_stats['project'] = name
                user_stats['project_category'] = category
                user_stats['project_commits'] = count 
            
            # retrieve primary file
            print("Retrieving primary file for %s" % u)
            cursor.execute(PRIMARY_FILE % (u))
            for (name, count) in cursor:
                user_stats['file'] = name
                user_stats['file_commits'] = count
            
            # retrieve most common word in commit comment
            print("Retrieving most common word in commit comment for %s" % u)
            cursor.execute(COMMIT_COMMENT_WORD % (u))
            text = ""
            for (comment) in cursor:
                text = text + " " + str(comment)
            user_stats['commit_word'] = find_most_common_word(text, user_stats['username'], user_stats['project'])
            
            # retrieve most common word in jira description
            print("Retrieving most common word in jira description for %s" % u)
            cursor.execute(JIRA_DESCRIPTION_WORD % (u))
            text = ""
            for (description) in cursor:
                text = text + " " + str(description)
            user_stats['jira_word'] = find_most_common_word(text, user_stats['username'], user_stats['project'])
            
            print(user_stats)
            cluster_stats.append(user_stats)
            
        for u in cluster_stats:
            if ('username' in u and 
                'project' in u and 
                'project_category' in u and 
                'project_commits' in u and 
                'file' in u and 
                'file_commits' in u and 
                'commit_word' in u and 
                'jira_word' in u):
                    output_file.write(u['username'] + "\t" + 
                                      u['project'] + "\t" + 
                                      str(u['project_category']) + "\t" +
                                      str(u['project_commits']) + "\t" + 
                                      u['file'] + "\t" + 
                                      str(u['file_commits']) + "\t" +
                                      u['commit_word'] + "\t" + 
                                      u['jira_word'] + "\n")
                                  
        output_file.write("\n")
        
    output_file.close()
    
    close_connection()
    
if __name__ == '__main__':
    main()