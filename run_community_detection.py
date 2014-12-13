#!usr/bin/env python

import mysql.connector, igraph, random, dbm

CNX = None
EMAIL_FILE = ""

QUERY_USERNAME = (
    "SELECT author_username, author_fullname, author_email "
    "FROM jira_comment "
    "WHERE author_username='%s' "
    "LIMIT 1"
)

def open_connection():
    global CNX
    print("Opening connection to database.")
    db = dbm.open('db_credentials', 'c')
    
    # Check for db credentials
    if not 'user' in db or not 'password' in db or not 'database' in db:
        print("Failed to find database credentials.")
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
    return True

def get_user_info(username):
    res = {}
    cursor = CNX.cursor()
    cursor.execute(QUERY_USERNAME % (username))
    for (username, fullname, email) in cursor:
        res['username'] = username
        res['fullname'] = fullname
        res['email'] = email
    
    return res
    
def generate_email(user, rank_users):
    global EMAIL_FILE
    
    db = dbm.open('email_content', 'c')
    email_body = db['body'].decode("utf-8")
    email_body = email_body % (user['username'], user['fullname'], user['email'],
                               rank_users[0]['weight'], rank_users[0]['username'], rank_users[0]['fullname'],
                               rank_users[1]['weight'], rank_users[1]['username'], rank_users[1]['fullname'],
                               rank_users[2]['weight'], rank_users[2]['username'], rank_users[2]['fullname'],
                               rank_users[3]['weight'], rank_users[3]['username'], rank_users[3]['fullname'],
                               rank_users[4]['weight'], rank_users[4]['username'], rank_users[4]['fullname'],
                               rank_users[5]['weight'], rank_users[5]['username'], rank_users[5]['fullname'])
                               
    EMAIL_FILE = EMAIL_FILE + email_body
    
    
def generate_rank_users(user_index, user_cluster, all_clusters):
    # List of users for email recipient to rank
    rank_users = []

    # Get user info
    username = user_cluster.vs[user_index].attributes()['label']
    user = get_user_info(username)
    
    # Get three random users from same cluster
    indices = set(range(0, user_cluster.vcount()))
    indices.remove(user_index)
    random_indices = random.sample(indices, 3)
    for i in random_indices:
        new_username = user_cluster.vs[i].attributes()['label']
        new_user = get_user_info(new_username)
        
        v1 = user_cluster.vs.find(label=username)
        v2 = user_cluster.vs.find(label=new_username)
        
        new_user['weight'] = user_cluster[v1.index,v2.index]
            
        rank_users.append(new_user)
    
    # Get three random users from other clusters
    indices = set(range(0, len(all_clusters)))
    indices.remove(all_clusters.index(user_cluster))
    random_indices = random.sample(indices, 3)
    for i in random_indices:
        random_index = random.randint(0, all_clusters[i].vcount()-1)
        new_username = all_clusters[i].vs[random_index].attributes()['label']
        new_user = get_user_info(new_username)
        new_user['weight'] = -1
        
        rank_users.append(new_user)
    
    generate_email(user, rank_users)

def write_emails():
    email_file = open("email_output.txt", "w")
    email_file.write(EMAIL_FILE)
    email_file.close()

def write_community_data(clusters):
    community_data_file = open("community_data_output.txt", "w")
    for c in clusters:
        for i in range(0, c.vcount()):
            community_data_file.write(c.vs[i].attributes()['label'] + "\n")
        community_data_file.write("\n")
    community_data_file.close()

def main():
    print("Running python-igraph version", igraph.__version__)
    
    # Open connection to database
    open_connection()
    
    # Read graph from GML file
    g = igraph.Graph.Read_GML("asf_data.gml")

    # Extract largest connected component
    giant = g.components().giant()

    # Run community detection algorithm on graph; return vertex dendrogram 
    vd = giant.community_edge_betweenness()

    # Cut dendrogram at optimal level 
    vc = vd.as_clustering()

    # Get all subgraphs belonging to each of the clusters
    clusters = vc.subgraphs()

    # Remove clusters with less than 5 vertices
    clusters = [c for c in clusters if c.vcount() >= 5]
    
    # Select three random users from each cluster to create e-mail
    for c in clusters:
        users = random.sample(list(range(0, c.vcount())), 3)
        for u in users:
            generate_rank_users(u, c, clusters)
    
    # Save email data to disk
    write_emails()
    
    # Save community data returned by algorithm to disk
    write_community_data(clusters)
    
    # Close connection to database
    close_connection()
    
if __name__ == '__main__':
    main()