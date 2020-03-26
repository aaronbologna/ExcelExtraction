# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 10:59:12 2020

@author: Aaron
"""

import pandas as pd
import os
from pathlib import Path

import xml.etree.ElementTree as ET
import sqlite3

pd.set_option('max_columns',None)
pd.set_option('max_rows',None)
pd.set_option('display.max_colwidth',None)

conn = sqlite3.connect('ecn_data.sqlite')
cur = conn.cursor()

# Make some fresh tables using executescript()
cur.executescript('''
DROP TABLE IF EXISTS ECN;
DROP TABLE IF EXISTS AFFECTED_PARTS;
DROP TABLE IF EXISTS AFFECTED_DOCUMENTS;
DROP TABLE IF EXISTS AFFECTED_FGS;


/* creates new tables */
CREATE TABLE ECN (
/* sets up the primary key so that it cannot be empty and that it auto increments upon the creation of a new record, also must be unique */
/*id and name are the column headers */
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    ecn   TEXT UNIQUE,
    eng TEXT,
    admin TEXT,
    class_of_change TEXT,
    reason TEXT,
    ref_ecr TEXT,
    ref_ecn TEXT,
    ref_ewo TEXT,
    date TEXT,
    description TEXT,
    severity TEXT
);
CREATE TABLE AFFECTED_DOCUMENTS (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    ecn   TEXT,
    pn TEXT,
    pn_rev_from TEXT,
    pn_rev_to TEXT,
    pn_dwg TEXT,
    pn_dwg_rev_from TEXT,
    pn_dwg_rev_to TEXT,
    pn_desc TEXT,
    d_onorder TEXT,
    d_onhand TEXT,
    d_wip TEXT,
    d_fgs TEXT
);
CREATE TABLE AFFECTED_FGS (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    ecn   TEXT,
    pn TEXT,
    model_name TEXT
)
''')

#location of all the ECN files to be imported
directory = r'C:\Users\Aaron\Desktop\ECNs'

#setting up a list of the names of all the files to iterate through 
files = []
for r, d, f in os.walk(directory):
     for file in f:
         files.append(os.path.join(r, file))     
# print('File List:')
# print(files)
#failed files to print out a list of everything that wasn't imported at the end
failed_files = []


for f in files:

    try: 
#pulling in all of the base information which will go into the ECN table
#values are mapped to their standard locations on the ECN excel files
        df = pd.read_excel (f, sheet_name='ECN Page 1')
        print(f)
#pnrow for to be used to iterate through affected documents
        pnrow=121
        
        ecn = list(df)[35]
        eng = df['Unnamed: 6'][8]
        pm = df['Unnamed: 19'][8]
        admin = df['Unnamed: 32'][8]
        class_of_change = df['Unnamed: 5'][26]
        reason = df['Unnamed: 8'][29]
        ref_ecr = df['ENGINEERING CHANGE NOTICE'][5]
        ref_ecn = df['Unnamed: 19'][5]
        ref_ewo = df['Unnamed: 25'][5]
        date = df['ECN #'][5]
#needed to strip due to leading new lines, prevented value from being written to DB
        description = df['CRESTRON ELECTRONICS'][41].strip()
        severity = df['Unnamed: 9'][104].strip()
        
 
#sql to write values into ECN table       
        cur.execute('''INSERT OR IGNORE INTO ECN (ecn, eng, admin, class_of_change, reason, ref_ecr, ref_ecn, ref_ewo, date, description, severity) 
            VALUES ( ?,?,?,?,?,?,?,?,?,?,? )''', (ecn, eng, admin, class_of_change, reason, ref_ecr, ref_ecn, ref_ewo, date, description, severity) )
          
        conn.commit()

#iterating through the affected documents 
        while True:
#checking for sting with length of 7 but probably can just make this check for null
            if len(str(df['Unnamed: 2'][pnrow])) == 7:
                
                pn = df['Unnamed: 2'][pnrow]
                pn_rev_from = df['Unnamed: 6'][pnrow]
                pn_rev_to = df['Unnamed: 7'][pnrow]
                pn_dwg = df['Unnamed: 8'][pnrow]
                pn_dwg_rev_from = df['ENGINEERING CHANGE NOTICE'][pnrow]
                pn_dwg_rev_to = df['Unnamed: 14'][pnrow]
                pn_desc = df['Unnamed: 15'][pnrow]
                d_onorder = df['Unnamed: 26'][pnrow]
                d_onhand = df['Unnamed: 28'][pnrow]
                d_wip = df['ECN #'][pnrow]
                d_fgs = df['Unnamed: 32'][pnrow]
                
                cur.execute('''INSERT OR IGNORE INTO AFFECTED_DOCUMENTS (ecn, pn, pn_rev_from, pn_rev_to, pn_dwg, pn_dwg_rev_from, pn_dwg_rev_to, pn_desc, d_onorder, d_onhand, d_wip, d_fgs) 
                            VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?)''', (ecn, pn, pn_rev_from, pn_rev_to, pn_dwg, pn_dwg_rev_from, pn_dwg_rev_to, pn_desc, d_onorder, d_onhand, d_wip, d_fgs))
                          
                conn.commit()
#increment by 4 since the excel file is using merged cells
                pnrow = pnrow + 4
                 
            else:
                break
#incrementing through the additional affected documents on the second page
#adds to the additional docs tables
        pnrow=1
        try:
            df2 = pd.read_excel (f, sheet_name='ECN PAGE 2')
            if list(df2)[1] =='ADDITIONAL DOCUMENTS AFFECTED:':
                while True: #this is only working for 21473,something wrong with 494
                    if len(str(df2['ADDITIONAL DOCUMENTS AFFECTED:'][pnrow])) == 7:
                        pn = df2['ADDITIONAL DOCUMENTS AFFECTED:'][pnrow]
                        pn_rev_from = df2['Unnamed: 2'][pnrow]
                        pn_rev_to = df2['Unnamed: 3'][pnrow]
                        pn_dwg = df2['Unnamed: 4'][pnrow]
                        pn_dwg_rev_from = df2['Unnamed: 5'][pnrow]
                        pn_dwg_rev_to = df2['Unnamed: 6'][pnrow]
                        pn_desc = df2['Unnamed: 7'][pnrow]
                        d_onorder = df2['Unnamed: 8'][pnrow]
                        d_onhand = df2['Unnamed: 9'][pnrow]
                        d_wip = df2['Unnamed: 10'][pnrow]
                        d_fgs = df2['Unnamed: 11'][pnrow]
                        cur.execute('''INSERT OR IGNORE INTO AFFECTED_DOCUMENTS (ecn, pn, pn_rev_from, pn_rev_to, pn_dwg, pn_dwg_rev_from, pn_dwg_rev_to, pn_desc, d_onorder, d_onhand, d_wip, d_fgs) 
                                    VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?)''', (ecn, pn, pn_rev_from, pn_rev_to, pn_dwg, pn_dwg_rev_from, pn_dwg_rev_to, pn_desc, d_onorder, d_onhand, d_wip, d_fgs))         
                        conn.commit()
#no merged cells being used, increment by 1
                        pnrow = pnrow + 1
                    else: break
            else: print('ECN Page 2 Format Error')
        except: print('No ECN Page 2')
#pulling in affected fgs to the fg table
        fgrow=0
        try:
           df3 = pd.read_excel (f, sheet_name='FG List') 
           while True:
               if len(str(int(df3['SAP #'][fgrow]))) == 7:
                   pn = df3['SAP #'][fgrow]
                   model_name = df3['Model Name'][fgrow]
                   cur.execute('''INSERT OR IGNORE INTO AFFECTED_FGS (ecn, pn, model_name) VALUES ( ?,?,?)''', (ecn, pn, model_name))
                   conn.commit()
                   fgrow = fgrow + 1
               else: break
        except: pass 
#summary information
        print('Additional PNs Added:',pnrow-1)
        print('FGs Affected:',fgrow) 
        print('Successful Import\n')
           
      
    except:
        failed_files.append(f)
        print(f,'\nFailed Import\n')

print('Failed Imports')
for failure in failed_files:
    print(failure)

      



























