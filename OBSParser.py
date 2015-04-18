#!/usr/bin/env python

# Copyright (C) 2015 

# Authors:
# Athanasios-Ilias Rousinopoulos <athanrous@gmail.com>, <zoumpis@opensuse.org>

import xmltodict
import sys
import MySQLdb
import requests
import hashlib
import bz2
import os
from dateutil import parser
from datetime import datetime
import matplotlib.pyplot as plt
from dateutil import parser
from bs4 import BeautifulSoup



class OBS:
    
    def __init__(self):
        
        print ""
               
    def OBStoXMl(self,date_in,date_out,repo_path,xml_path,obs_project):
        
        
        if not os.path.isfile(xml_path):
            
            os.system("python '%s' api '/search/request?match=state/@when>='%s'+and+state/@when<'%s'+and+action/target/@project='%s' > '%s''" %(repo_path,date_in,date_out,obs_project,xml_path))
       
        else:
            print ("Create another file")
         
        return xml_path
        

class OBSParser:
    
    def __init__(self,path,db_name,credentials):
        
        self.path = path
        self.db_name = db_name
        self.credentials = credentials
        
        
    def return_dict(self):
        
        """
        This generates a dict of our xml
        Requests is now a list, but we do not want a list
        For every element in the requests list, we parse and 
        init our data as a dict, where the key of each
        element is the request @id """
        
        filepath = self.path
        data = {}

        requests = []

        with open(filepath) as f:
            xml = f.read()

    
        for req in requests:
            data[req['@id']] = req

        return data

    def find_ids(self,data):
        
        """
        Takes a dict, that was previously initiliazed
        and finds all information about the requests.

        Returns a list of all the requests ids
        """
        requests_made = len(data.keys())
        print "---------"
        print str(requests_made) + " requests made"
        print "---------"
        print " "

        ids = data.keys()
        return ids
       
    def GetCredentials(self):
       
       #Your xml file for OBS credentials has to follow the following format :
       # <person>
       #     <username>username</login>
       #     <password>password</email>
       # </person>
       
        data = {}

        username = []
        password = []
        credential = []
        with open(self.credentials) as f:
            xml = f.read()

        doc = xmltodict.parse(xml)
        username = doc['person']['username']
        password = doc['person']['password'] 
        encrypted_password = bz2.compress(password)
        pass_final = bz2.decompress(encrypted_password)
        credential.append(username)
        credential.append(pass_final) 
        return credential
        
    def GetHistory(self,data):
        
        """
        Takes a dict and returns all the history data.
        This is rather tricky, histories could be dictionaries
        or lists of dictionaries. 
    
        """
        db = MySQLdb.connect("localhost","root","root")
        cursor = db.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS %s;" %(self.db_name))
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS ids ( id INT NOT NULL,PRIMARY KEY (id) ) ENGINE=INNODB;") 
        cursor.execute("CREATE TABLE IF NOT EXISTS history (name VARCHAR(30),username VARCHAR(30),email VARCHAR(50),date DATE,time TIME,md5_id VARCHAR(32),parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        username = self.GetCredentials()[0]
        password = self.GetCredentials()[1]
        request_id = data['@id']
        print "HISTORY DATA FOR REQUEST WITH ID: %s" % request_id
        cursor.execute("INSERT INTO ids (id) VALUES ('%s') ;" %(request_id))
        history = data['history']
              
        if type(history) == list:
            print "Found multiple history elements for this request"
            print ""
            for hist in history:
                date = parser.parse(hist['@when']).date()
                time = parser.parse(hist['@when']).time()
                email = self.GetUserEmail(hist['@who'],username,password)
                md5_id = hashlib.md5(hist['@who']).hexdigest()
                cursor.execute("INSERT INTO history (name,username,email,date,time,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');" % (hist['@name'],hist['@who'],email,date,time,md5_id,request_id))
                db.commit()
            
        else:
            date_n = parser.parse(history['@when']).date()
            time_n = parser.parse(history['@when']).time()
            email_n = self.GetUserEmail(history['@who'],username,password)
            md5_id_n = hashlib.md5(history['@who']).hexdigest() 
            cursor.execute("INSERT INTO history (name,username,email,date,time,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');" % (history['@name'],history['@who'],email_n,date_n,time_n,md5_id_n,request_id))
            db.commit()
            db.close()
                
                
        print "------------------------------------"
        
    def GetReviews(self,data):
        
        db = MySQLdb.connect("localhost","root","root","%s" %(self.db_name))
        cursor = db.cursor() 
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS reviews (state VARCHAR(255),username VARCHAR(255),email VARCHAR(50),date DATE,time TIME,by_group VARCHAR(255),md5_id VARCHAR(255) NOT NULL, parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        username = self.GetCredentials()[0]
        password = self.GetCredentials()[1]
        request_id = data['@id']
        print "REVIEW DATA FOR REQUEST WITH ID: %s" % request_id
        
        try:
            review = data['review']
            
            if type(review) == list:
                print "Found multiple history elements for this request"
                print ""
                for rev in review:
                    date = parser.parse(rev['@when']).date()
                    time = parser.parse(rev['@when']).time()
                    email = self.GetUserEmail(rev['@who'],username,password)
                    md5_id = hashlib.md5(rev['@who']).hexdigest()
                    cursor.execute("INSERT INTO reviews (state,username,email,date,time,by_group,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');" % (rev['@state'],rev['@who'],email,date,time,md5_id,rev['@by_group'],request_id))
                    db.commit()
                    
            else:

                date_n = parser.parse(review['@when']).date()
                time_n = parser.parse(review['@when']).time()
                md5_id_n = hashlib.md5(review['@who']).hexdigest()
                email_n = self.GetUserEmail(review['@who'],username,password)
                cursor.execute("INSERT INTO reviews (state,username,email,date,time,by_group,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');" % (review['@state'],review['@who'],email_n,date_n,time_n,md5_id_n,review['@by_group'],request_id))
                db.commit()
            db.close()
            
        except KeyError:
            pass
        
        print "------------------------------------" 

    def GetState(self,data):    
    
        request_id = data['@id']
        state = data['state']
        db = MySQLdb.connect("localhost","root","root","%s" %(self.db_name))
        cursor = db.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS state (name VARCHAR(30),username VARCHAR(255),email VARCHAR(50),date DATE,time TIME,md5_id VARCHAR(32), parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        username = self.GetCredentials()[0]
        password = self.GetCredentials()[1]
        if type(state) == list:
            
            print "Found multiple history elements for this request"
            for sta in state:    
                date = parser.parse(sta['@when']).date()
                time = parser.parse(sta['@when']).time()
                email = self.GetUserEmail(sta['@who'],username,password)
                md5_id = hashlib.md5(sta['@who']).hexdigest()
                cursor.execute("INSERT INTO state (name,username,email,date,time,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s',%s');" % (sta['@name'],sta['@who'],email,date,time,md5_id,request_id))
                db.commit()
        else:
            date_n = parser.parse(state['@when']).date()
            time_n = parser.parse(state['@when']).time()
            email_n = self.GetUserEmail(state['@who'],username,password)
            md5_id_n = hashlib.md5(state['@who']).hexdigest()
            cursor.execute("INSERT INTO state (name,username,email,date,time,md5_id,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');" % (state['@name'],state['@who'],email_n,date_n,time_n,md5_id_n,request_id))
            db.commit()
            
        
        db.close()
  
    def GetSource(self,data):
        
        request_id = data['@id']
        db = MySQLdb.connect("localhost","root","root","%s" %(self.db_name))
        cursor = db.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS source (project VARCHAR(255),package VARCHAR(255),rev VARCHAR(255), parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        
        try:
            source = data['action']['source']
        except TypeError:
            print 'This is a TypeError'
            return 
        except KeyError:
            print "The request with id: [%s] has no source element" % request_id
            return
        
        if type(source) == list:
            print "Found multiple history elements for this request"
            for sou in source:
                
                if not '@rev' in sou.keys():
                    print "No revision found"
                    sou['@rev'] = "No Revision Found"
                else:
                    print "Rev:", sou['@rev']

                print ""
                cursor.execute("INSERT INTO source (project,package,rev,parent_id) VALUES ('%s','%s','%s','%s');" % (sou['@project'],sou['@package'],sou['@rev'],request_id))
                db.commit()
        else:
            print "Got into this"
            print request_id
            if not '@rev' in source.keys():
                print "No revision found"
                source['@rev'] = "No Revision Found"
            else:
                print "Rev:", source['@rev']
            print ""
            cursor.execute("INSERT INTO source (project,package,rev,parent_id) VALUES ('%s','%s','%s','%s');" % (source['@project'],source['@package'],source['@rev'],request_id))
            db.commit()
            
        db.close()
        
    def GetTarget(self,data):    
    
        request_id = data['@id']
        target = data['action']
        db = MySQLdb.connect("localhost","root","root","%s" %(self.db_name))
        cursor = db.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS target (project VARCHAR(255),package VARCHAR(255),rev INT, parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
    
        try:

            if type(target) == list:
                print "--->Found multiple history elements for this request"
                print ""
                for tar in target:
                    cursor.execute("INSERT INTO target (project,package,parent_id) VALUES ('%s','%s','%s');" % (tar['target']['@project'],tar['target']['@package'],request_id))
                    db.commit()
            else:
                
                cursor.execute("INSERT INTO target (project,package,parent_id) VALUES ('%s','%s','%s');" % (target['target']['@project'],target['target']['@package'],request_id))
                db.commit()

            db.close()

        except KeyError:
            return
        
    def GetAction(self,data):    
    
        request_id = data['@id']
        action = data['action']
        db = MySQLdb.connect("localhost","root","root","%s" %(self.db_name))
        cursor = db.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS action (type VARCHAR(30),parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
    
        if type(action) == list:
            print "Found multiple history elements for this request"
            print ""
            for act in action:
                
                cursor.execute("INSERT INTO action(type,parent_id) VALUES ('%s','%s');" % (act['@type'],request_id))
                db.commit()
        else:
            
            cursor.execute("INSERT INTO action(type,parent_id) VALUES ('%s','%s');" % (action['@type'],request_id))
            db.commit()
            
        db.close()
    
    def GetUserEmail(self,user,username,password):
        
        r = requests.get('https://api.opensuse.org/person/%s?format=XML' %user, auth=(username, password))
        r.text.encode('ascii','ignore')
        soup_code = BeautifulSoup(r.text,"xml")
        for p in soup_code.find_all('email'):
            print p.contents[0]
            return p.contents[0]
     
    def PlotDB(self):
        
        #This query obtains all the submitted requests in OSC
        #For queries with categorized by state add a "where name = 'revoked' or 'accepted' or 'superseded' or 'declined'  stament" 
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()  
        query_act_rev = "select count(parent_id) from reviews GROUP BY year(date), month(date), day(date) ORDER BY date desc;"
        cursor.execute(query_act_rev)
        activity_rev_requests = [int(item[0]) for item in cursor.fetchall()]
        
        query_date_rev = "select count(parent_id) from reviews GROUP BY year(date), month(date), day(date) ORDER BY date desc;"
        cursor.execute(query_act_rev)
        date_rev_requests = [int(item[0]) for item in cursor.fetchall()] 
        plt.plot_date(date_rev_requests, activity_rev_requests, linestyle='-', xdate=True, ydate=False,color='r')
        plt.show()
            
class Log():    
    
    
    
    def __init__(self,db_name,project_name):
        
        
        self.db_name = db_name
        self.project_name = project_name 
        
    
    def GetState(self,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select name,username,email,date,time from state where parent_id = '%s'" %id)
        state = cursor.fetchall()
        con.close()
        return state

     
    def GetHistory(self,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select name,username,email,date,time from history where parent_id = '%s'" %id)
        history = cursor.fetchall()
        
        con.close()
        return history
    
    
    def GetReviewHistoryID(self,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select state,username,email,date,time from reviews where parent_id = '%s'" %id)
        
        review_history = cursor.fetchall()
        con.close()
        return review_history
    
    
    def GetSource(self,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select * from source where parent_id = '%s'" %id)
        
        source = cursor.fetchall()
        con.close()
        return source    
     
    def GetIDsProject(self,project_name):
         
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select distinct(st.parent_id) from source s,state st where s.project='%s' and st.name = 'accepted' and s.parent_id = st.parent_id;" %project_name)
       
        ids = []
        elements = cursor.fetchall()
        con.close()
        
        for item in range(len(elements)):
            ids.append(int(elements[item][0]))
            
        return ids
    
    def CreateLog(self,project_name,id):
        
        """Create an extra table cold log and insert the information there"""
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS log (name VARCHAR(30),username VARCHAR(30),email VARCHAR(50),date DATE,time TIME,project VARCHAR(255),package VARCHAR(255),parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
                
        """Get the all the rows from the source table.After applying heuristics this is the best approach"""
        
        try:
            source = self.GetSource(id)
            if source != 0:
        
            
                s_project = source[0][0]
                s_package = source[0][1]
            
        except ValueError:
            print 'This is a TypeError'
            return 
        
        """Get the first row from the history table.After applying heuristics this is the best approach"""
        
        history = self.GetHistory(id)
        if history is not None :
        
            h_name = history[0][0]
            h_username = history[0][1]
            h_email = history[0][2]
            h_date = history[0][3]
            h_time = history[0][4]
            cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (h_name,h_username,h_email,h_date,h_time,s_project,s_package,id))
            con.commit()
                
        """Get the all the rows from the reviews table.After applying heuristics this is the best approach"""
            
        reviews = self.GetReviewHistoryID(id)
            
        if len(reviews) == 0:
                
            print 'This is a TypeError' , len(reviews), id    
            return
            
        elif len(reviews) == 1:
            r_state = reviews[0][0]
            r_username = reviews[0][1]
            r_email = reviews[0][2]
            r_date = reviews[0][3]
            r_time = reviews[0][4]
            cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
            con.commit()
                
        elif len(reviews) == 2:
                
            r_state = reviews[0][0]
            r_username = reviews[0][1]
            r_email = reviews[0][2]
            r_date = reviews[0][3]
            r_time = reviews[0][4]
            r_state_sec = reviews[1][0]
            r_username_sec = reviews[1][1]
            r_email_sec = reviews[1][2]
            r_date_sec = reviews[1][3]
            r_time_sec = reviews[1][4]
            cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
            con.commit()
            cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_sec,r_username_sec,r_email_sec,r_date_sec,r_time_sec,s_project,s_package,id))
            con.commit()
                
        elif len(reviews) ==3:
                
                
             r_state = reviews[0][0]
             r_username = reviews[0][1]
             r_email = reviews[0][2]
             r_date = reviews[0][3]
             r_time = reviews[0][4]
             r_state_sec = reviews[1][0]
             r_username_sec = reviews[1][1]
             r_email_sec = reviews[1][2]
             r_date_sec = reviews[1][3]
             r_time_sec = reviews[1][4]
             r_state_tri = reviews[2][0]
             r_username_tri = reviews[2][1]
             r_email_tri = reviews[2][2]
             r_date_tri = reviews[2][3]
             r_time_tri = reviews[2][4]   
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_sec,r_username_sec,r_email_sec,r_date_sec,r_time_sec,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_tri,r_username_tri,r_email_tri,r_date_tri,r_time_tri,s_project,s_package,id))
             con.commit()
            
        elif len(reviews) == 4 :
                
                
             r_state = reviews[0][0]
             r_username = reviews[0][1]
             r_email = reviews[0][2]
             r_date = reviews[0][3]
             r_time = reviews[0][4]
             r_state_sec = reviews[1][0]
             r_username_sec = reviews[1][1]
             r_email_sec = reviews[1][2]
             r_date_sec = reviews[1][3]
             r_time_sec = reviews[1][4]
             r_state_tri = reviews[2][0]
             r_username_tri = reviews[2][1]
             r_email_tri = reviews[2][2]
             r_date_tri = reviews[2][3]
             r_time_tri = reviews[2][4]
             r_state_cat = reviews[3][0]
             r_username_cat = reviews[3][1]
             r_email_cat = reviews[3][2]
             r_date_cat = reviews[3][3]
             r_time_cat = reviews[3][4]   
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_sec,r_username_sec,r_email_sec,r_date_sec,r_time_sec,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_tri,r_username_tri,r_email_tri,r_date_tri,r_time_tri,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_cat,r_username_cat,r_email_cat,r_date_cat,r_time_cat,s_project,s_package,id))
             con.commit()
            
        elif len(reviews) == 5 :
                
                
             r_state = reviews[0][0]
             r_username = reviews[0][1]
             r_email = reviews[0][2]
             r_date = reviews[0][3]
             r_time = reviews[0][4]
             r_state_sec = reviews[1][0]
             r_username_sec = reviews[1][1]
             r_email_sec = reviews[1][2]
             r_date_sec = reviews[1][3]
             r_time_sec = reviews[1][4]
             r_state_tri = reviews[2][0]
             r_username_tri = reviews[2][1]
             r_email_tri = reviews[2][2]
             r_date_tri = reviews[2][3]
             r_time_tri = reviews[2][4]   
             r_state_cat = reviews[3][0]
             r_username_cat = reviews[3][1]
             r_email_cat = reviews[3][2]
             r_date_cat = reviews[3][3]
             r_time_cat = reviews[3][4]    
             r_state_fif = reviews[4][0]
             r_username_fif = reviews[4][1]
             r_email_fif = reviews[4][2]
             r_date_fif = reviews[4][3]
             r_time_fif = reviews[4][4]   
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_sec,r_username_sec,r_email_sec,r_date_sec,r_time_sec,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_tri,r_username_tri,r_email_tri,r_date_tri,r_time_tri,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_cat,r_username_cat,r_email_cat,r_date_cat,r_time_cat,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_fif,r_username_fif,r_email_fif,r_date_fif,r_time_fif,s_project,s_package,id))
             con.commit()
             
        elif len(reviews) == 6 :
                
                
             r_state = reviews[0][0]
             r_username = reviews[0][1]
             r_email = reviews[0][2]
             r_date = reviews[0][3]
             r_time = reviews[0][4]
             r_state_sec = reviews[1][0]
             r_username_sec = reviews[1][1]
             r_email_sec = reviews[1][2]
             r_date_sec = reviews[1][3]
             r_time_sec = reviews[1][4]
             r_state_tri = reviews[2][0]
             r_username_tri = reviews[2][1]
             r_email_tri = reviews[2][2]
             r_date_tri = reviews[2][3]
             r_time_tri = reviews[2][4]   
             r_state_cat = reviews[3][0]
             r_username_cat = reviews[3][1]
             r_email_cat = reviews[3][2]
             r_date_cat = reviews[3][3]
             r_time_cat = reviews[3][4]   
             r_state_fif = reviews[4][0]
             r_username_fif = reviews[4][1]
             r_email_fif = reviews[4][2]
             r_date_fif = reviews[4][3]
             r_time_fif = reviews[4][4]
             r_state_six = reviews[5][0]
             r_username_six = reviews[5][1]
             r_email_six = reviews[5][2]
             r_date_six = reviews[5][3]
             r_time_six = reviews[5][4]    
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state,r_username,r_email,r_date,r_time,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_sec,r_username_sec,r_email_sec,r_date_sec,r_time_sec,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_tri,r_username_tri,r_email_tri,r_date_tri,r_time_tri,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_cat,r_username_cat,r_email_cat,r_date_cat,r_time_cat,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_fif,r_username_fif,r_email_fif,r_date_fif,r_time_fif,s_project,s_package,id))
             con.commit()
             cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (r_state_six,r_username_six,r_email_six,r_date_six,r_time_six,s_project,s_package,id))
             con.commit()
            
              
        else:
            print 'This is a TypeError' , len(reviews)
            return
       
        """Get the all the state from the reviews table.After applying heuristics this is the best approach"""
        
        state = self.GetState(id)
        
        if state is not None :
            
            st_name = state[0][0]
            st_username = state[0][1]
            st_email = state[0][2]
            st_date = state[0][3]
            st_time = state[0][4]
            cursor.execute("INSERT INTO log(name,username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s');"  % (st_name,st_username,st_email,st_date,st_time,s_project,s_package,id))
            con.commit()
        
           
        con.close()
        return 
    
    def GetRequestDates(self,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select date,time,project,package from log where parent_id = '%s';" %id)
        req_info = cursor.fetchall()
        con.close()
        return req_info
        
    def GetDiffTime(self,project_name,id):
        
        """This function is used for gathering time differnce between the first and the last action in a submitted (and accepted) request."""
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        info = self.GetRequestDates(id)
        k = len(info) 
       
        print k 
        for i in range(len(info)):
            
            dt_first = info[0][0] 
            tm_first = info[0][1]
            pro_first = info[0][2]
            pack_first = info[0][3]
            dt_last = info[k-1][0]
            tm_last = info[k-1][1]
            
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select concat('%s',' ' ,'%s')  as dt from log where parent_id ='%s';" %(dt_first,tm_first,id))
        date_first = cursor.fetchall()
        date_sf = ' '.join(map(str, (date_first[0])))
        first = datetime.strptime(date_sf, "%Y-%m-%d %H:%M:%S")
        cursor.execute("select concat('%s',' ' ,'%s')  as dt from log where parent_id ='%s';" %(dt_last,tm_last,id)) 
        date_last = cursor.fetchall()
        date_st = ' '.join(map(str, (date_last[0])))
        last = datetime.strptime(date_st, "%Y-%m-%d %H:%M:%S")
        diff = []
        diff_days = (last - first).days
        diff_sec = (last - first).seconds
        diff_min = diff_sec / 60
        diff.append(diff_days)
        diff.append(diff_min)
        diff.append(diff_sec)
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS deltas (days INT NOT NULL,minutes INT NOT NULL,seconds INT NOT NULL,project VARCHAR(255),package VARCHAR(255), parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        cursor.execute("INSERT INTO deltas (days,minutes,seconds,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s');" % (diff[0],diff[1],diff[2],pro_first,pack_first,id))
        con.commit()
        con.close() 
        return diff
   
    def GetRequestLog(self,project_name,id):
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select username,email,date,time,project,package,parent_id from log where parent_id = '%s' and name = 'accepted' and project = '%s';" %(id,project_name))
        req_log = cursor.fetchall()
        con.close()
        return req_log
    
    
    def ComputeStatistics(self,project_name):
        
        """This part will be handled via R. In the near future statistics can be computed directly via this method"""
        
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("select seconds from deltas where project = '%s';" %(project_name))
        req_log = cursor.fetchall()
        con.close()
        stat= []
        for i in range(len(req_log)):
            
            element = ' '.join(map(str, (req_log[i])))
            fin = int(element)
            stat.append(fin)
              
        return stat    
        
        
    
    def CreateReviewersLog(self,project_name,id):
        
        """This function creates a table where ONLY the review log information for each requests is stored. No reviewed requests are not being stored."""
 
        req_info = self.GetRequestLog(project_name,id)
        con = MySQLdb.connect('localhost', 'root', 'root', '%s' %(self.db_name))
        cursor = con.cursor()
        cursor.execute("USE %s;" %(self.db_name))
        cursor.execute("CREATE TABLE IF NOT EXISTS reviews_log (username VARCHAR(30),email VARCHAR(50),date DATE,time TIME,project VARCHAR(255),package VARCHAR(255),parent_id INT, INDEX par_ind (parent_id),FOREIGN KEY (parent_id) REFERENCES ids(id) ON DELETE CASCADE ) ENGINE=INNODB;")
        
        if len(req_info) == 0:
                
            print 'This is a TypeError' , len(req_info), id    
            return
            
        
        elif len(req_info) == 2:
                    
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            cursor.execute("INSERT INTO log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
                   
        elif len(req_info) == 3:
                  
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            r_username_sec = req_info[1][0]
            r_email_sec = req_info[1][1]
            r_date_sec = req_info[1][2]
            r_time_sec = req_info[1][3]
            r_project_sec = req_info[1][4]
            r_package_sec = req_info[1][5] 
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_sec,r_email_sec,r_date_sec,r_time_sec,r_project_sec,r_package_sec,id))
            con.commit()
                     
        elif len(req_info) == 4:
                 
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            r_username_sec = req_info[1][0]
            r_email_sec = req_info[1][1]
            r_date_sec = req_info[1][2]
            r_time_sec = req_info[1][3]
            r_project_sec = req_info[1][4]
            r_package_sec = req_info[1][5]
            r_username_tri = req_info[2][0]
            r_email_tri = req_info[2][1]
            r_date_tri = req_info[2][2]
            r_time_tri = req_info[2][3]
            r_project_tri = req_info[2][4]
            r_package_tri = req_info[2][5]
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_sec,r_email_sec,r_date_sec,r_time_sec,r_project_sec,r_package_sec,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_tri,r_email_tri,r_date_tri,r_time_tri,r_project_tri,r_package_tri,id))
            con.commit()
            
        elif len(req_info) == 5:
                        
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            r_username_sec = req_info[1][0]
            r_email_sec = req_info[1][1]
            r_date_sec = req_info[1][2]
            r_time_sec = req_info[1][3]
            r_project_sec = req_info[1][4]
            r_package_sec = req_info[1][5] 
            r_username_tri = req_info[2][0]
            r_email_tri = req_info[2][1]
            r_date_tri = req_info[2][2]
            r_time_tri = req_info[2][3]
            r_project_tri = req_info[2][4]
            r_package_tri = req_info[2][5]
            r_username_cat = req_info[3][0]
            r_email_cat = req_info[3][1]
            r_date_cat = req_info[3][2]
            r_time_cat = req_info[3][3]
            r_project_cat = req_info[3][4]
            r_package_cat = req_info[3][5]
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_sec,r_email_sec,r_date_sec,r_time_sec,r_project_sec,r_package_sec,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_tri,r_email_tri,r_date_tri,r_time_tri,r_project_tri,r_package_tri,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_cat,r_email_cat,r_date_cat,r_time_cat,r_project_cat,r_package_cat,id))
            con.commit()
                        
        elif len(req_info) == 6:
                        
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            r_username_sec = req_info[1][0]
            r_email_sec = req_info[1][1]
            r_date_sec = req_info[1][2]
            r_time_sec = req_info[1][3]
            r_project_sec = req_info[1][4]
            r_package_sec = req_info[1][5]
            r_username_tri = req_info[2][0]
            r_email_tri = req_info[2][1]
            r_date_tri = req_info[2][2]
            r_time_tri = req_info[2][3]
            r_project_tri = req_info[2][4]
            r_package_tri = req_info[2][5]
            r_username_cat = req_info[3][0]
            r_email_cat = req_info[3][1]
            r_date_cat = req_info[3][2]
            r_time_cat = req_info[3][3]
            r_project_cat = req_info[3][4]
            r_package_cat = req_info[3][5]
            r_username_fif = req_info[4][0]
            r_email_fif = req_info[4][1]
            r_date_fif = req_info[4][2]
            r_time_fif = req_info[4][3]
            r_project_fif = req_info[4][4]
            r_package_fif = req_info[4][5]
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_sec,r_email_sec,r_date_sec,r_time_sec,r_project_sec,r_package_sec,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_tri,r_email_tri,r_date_tri,r_time_tri,r_project_tri,r_package_tri,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_cat,r_email_cat,r_date_cat,r_time_cat,r_project_cat,r_package_cat,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_fif,r_email_fif,r_date_fif,r_time_fif,r_project_fif,r_package_fif,id))
            con.commit()
    
        elif len(req_info) == 7:
                       
            r_username = req_info[0][0]
            r_email = req_info[0][1]
            r_date = req_info[0][2]
            r_time = req_info[0][3]
            r_project = req_info[0][4]
            r_package = req_info[0][5]
            r_username_sec = req_info[1][0]
            r_email_sec = req_info[1][1]
            r_date_sec = req_info[1][2]
            r_time_sec = req_info[1][3]
            r_project_sec = req_info[1][4]
            r_package_sec = req_info[1][5]
            r_username_tri = req_info[2][0]
            r_email_tri = req_info[2][1]
            r_date_tri = req_info[2][2]
            r_time_tri = req_info[2][3]
            r_project_tri = req_info[2][4]
            r_package_tri = req_info[2][5]
            r_username_cat = req_info[3][0]
            r_email_cat = req_info[3][1]
            r_date_cat = req_info[3][2]
            r_time_cat = req_info[3][3]
            r_project_cat = req_info[3][4]
            r_package_cat = req_info[3][5]
            r_username_fif = req_info[4][0]
            r_email_fif = req_info[4][1]
            r_date_fif = req_info[4][2]
            r_time_fif = req_info[4][3]
            r_project_fif = req_info[4][4]
            r_package_fif = req_info[4][5]
            r_username_six = req_info[5][0]
            r_email_six = req_info[5][1]
            r_date_six = req_info[5][2]
            r_time_six = req_info[5][3]
            r_project_six = req_info[5][4]
            r_package_six = req_info[5][5]
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username,r_email,r_date,r_time,r_project,r_package,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_sec,r_email_sec,r_date_sec,r_time_sec,r_project_sec,r_package_sec,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_tri,r_email_tri,r_date_tri,r_time_tri,r_project_tri,r_package_tri,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_cat,r_email_cat,r_date_cat,r_time_cat,r_project_cat,r_package_cat,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_fif,r_email_fif,r_date_fif,r_time_fif,r_project_fif,r_package_fif,id))
            con.commit()
            cursor.execute("INSERT INTO reviews_log(username,email,date,time,project,package,parent_id) VALUES ('%s','%s','%s','%s','%s','%s','%s');"  % (r_username_six,r_email_six,r_date_six,r_time_six,r_project_six,r_package_six,id))
            con.commit()
    
        else:
            print 'This is a TypeError' , len(req_info)
            return
        
        con.close()
        return
    
def main():
    
    #First you have to crate the table log then the table deltas  
    #and then the table reviews_log.After that you can compute the statistics about deltas
  
   zpath = "your/path/to/xml/file"
   zcreds = "/credentials.xml"
   Parser = OBSParser(zpath, "Factory_Requests",zcreds)
   raw_input("Press Enter to parse the %s file" % zpath)
   zdata = Parser.return_dict()
   raw_input("Press Enter to find all request ids")
   zids = Parser.find_ids(zdata)
   raw_input("Press Enter to find all history elements for each id")
   
   for id in zids:
        
       Parser.GetHistory(zdata[id])
       Parser.GetAction(zdata[id])
        
       Parser.GetReviews(zdata[id])
       Parser.GetState(zdata[id])
        
       Parser.GetSource(zdata[id])
       Parser.GetTarget(zdata[id])
        
    
    
       print ""
    
id_list = Kde.GetIDsProject('KDE:Distro:Factory')
Kde = Log('Factory_Requests','KDE:Distro:Factory')
    
for id in id_list:
    
    Kde_log = Kde.CreateLog('KDE:Distro:Factory', id)

for id in id_list:
    
    Kde.CreateReviewersLog('KDE:Distro:Factory', id)
     
for id in id_list:    

    info  =  Kde.GetDiffTime('KDE:Distro:Factory',id)
        
if __name__ == '__main__':
    main()
    
   
   
    
