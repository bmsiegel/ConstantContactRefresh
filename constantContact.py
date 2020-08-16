import requests
import os
import base64
import json
import pandas as pd
import time
from datetime import date, datetime
from getAttachment import getAttachment

def getBasicAuth():     
        print('Reading Credentials...')
        with open('credentials.txt', 'r') as f:
                data = f.read().splitlines()
                client_id = data[2] 
                client_secret = data[3]
                refresh_token = data[4]
        credential = base64.b64encode('{}:{}'.format(client_id, client_secret).encode('ascii'))
        authData = {'Authorization':'Basic {}'.format(credential.decode())}
        print('Getting New Access Token...')
        r = requests.post('https://idfed.constantcontact.com/as/token.oauth2?refresh_token={}&grant_type=refresh_token'.format(refresh_token), headers = authData)
        authResponse = json.loads(r.content.decode())
        data[4] = authResponse['refresh_token']
        print('Updating Refresh Token...')
        with open('credentials.txt', 'w') as f:
                for d in data:
                        f.write(d + '\n')
        return authResponse['access_token']

def getJsonNewlyCreated(csv, lists):
        csv = csv.splitlines()
        fieldCount = len(csv[0].split(','))
        csv = csv[1:]
        result = dict()
        result['import_data'] = list()
        result['list_ids'] = lists
        for line in csv:
                fields = line.split(',')
                if len(fields) != fieldCount:
                        print('{} is malformed, please check input data'.format(line))
                        continue
                try:
                        birthDate = datetime.strptime(fields[13], '%m/%d/%Y')
                except:
                        birthDate = None

                member = {'email' : fields[11],
                          'first_name' : fields[2],
                          'last_name' : fields[1],
                          'birthday_month' : birthDate.month if birthDate else '', #getMonth
                          'birthday_day' : birthDate.day if birthDate else '', #getDay
                          'company_name' : fields[4],
                          'home_phone' : fields[10] if fields[10] != '' else fields[9]}
                result['import_data'].append(member)
        return result

def getJsonMemberTransactions(csv, lists):
        csv = csv.splitlines()
        fieldCount = len(csv[0].split(','))
        csv = csv[1:]
        result = dict()
        result['import_data'] = list()
        result['list_ids'] = lists
        for line in csv:
            fields = line.split(',')

            print(fields)

            #Pass function to make this part
            member = {'first_name' : fields[4].replace('\"', '').replace(' ', ''),
                      'last_name' : fields[3].replace('\"', '').replace(' ', ''),
                      'email' : fields[-1]
                     }

            result['import_data'].append(member)
        return result

def filterTable(table, column, target):
        return table[table[column] == target]

def filterToString(table, column, target):
        return table[table[column] == target].to_csv(index=False)

def bulkImport(authData, gmfJson, whJson, pJson, bcJson):
        r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=whJson)
        time.sleep(1)
        r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=bcJson)
        time.sleep(1)
        r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=gmfJson)
        time.sleep(1)
        r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=pJson)
        time.sleep(1)
        
if __name__ == '__main__':

        print('{}'.format(date.today()))
        
        goOn = False 

        subjects = {
                        'Newly Created':'newly_created.csv',
                        'Member Transactions':'member_transactions.csv'
                   }

        for i in subjects:
                if os.path.exists(subjects[i]):
                        os.remove(subjects[i])
                print('Getting Email Attachment with the Subject Line {}...'.format(i))
                attachmentExists = getAttachment(i, subjects[i])
                if not attachmentExists:
                        print('No Attachments')
                else:
                        print('Attachment Saved...')
                        goOn = True
        
        if not goOn: 
            print('No Member Updates, Done!')
            exit()

        auth = getBasicAuth()
        authData = {'Authorization' : 'Bearer {}'.format(auth)}
        token = {'token' : auth}
        
        print('Getting Contact List IDs...')
        r = requests.get('https://api.cc.email/v3/contact_lists', headers=authData)
        contact_lists = json.loads(r.content.decode())
        listIDs = dict()
        for l in contact_lists['lists']:
                if l['name'] == 'All Wheel House':
                        listIDs['WH'] = l['list_id']
                elif l['name'] == 'All Fit Body Boot Camp':
                        listIDs['BC'] = l['list_id']
                elif l['name'] == 'All Parisi':
                        listIDs['P'] = l['list_id']
                elif l['name'] == 'Gold Medal Fitness':
                        listIDs['GMF'] = l['list_id']
        
        print('Splitting New Members into Different Clubs...')          
        newMembers = pd.read_csv(subjects['Newly Created'])
        newWH = filterToString(newMembers, 'Club Nbr', 8656)
        newBC = filterToString(newMembers, 'Club Nbr', 8655)
        newGMF = filterToString(newMembers, 'Club Nbr', 8070)
        newP = filterToString(newMembers, 'Club Nbr', 8653)

        print('Getting JSON Data...')
        whJson = getJsonNewlyCreated(newWH, [listIDs['WH']])
        print(json.dumps(whJson, indent=2))
        bcJson = getJsonNewlyCreated(newBC, [listIDs['BC']])
        print(json.dumps(bcJson, indent=2))
        gmfJson = getJsonNewlyCreated(newGMF, [listIDs['GMF']])
        print(json.dumps(gmfJson, indent=2))
        pJson = getJsonNewlyCreated(newP, [listIDs['P']])
        print(json.dumps(pJson, indent=2))
        
        print('Sending New Members to CC...')
        bulkImport(authData, gmfJson, whJson, pJson, bcJson)
        
        editMembers = pd.read_csv(subjects['Member Transactions'])
        print('Filtering Demographic Changes...')
        editMembers = filterTable(editMembers, 'Change Type', 'Update Demographics')
        print('Getting Member Transactions into Different Clubs...')
        editWH = filterToString(editMembers, 'Club Nbr', 8656)
        editBC = filterToString(editMembers, 'Club Nbr', 8655)
        editGMF = filterToString(editMembers, 'Club Nbr', 8070)
        editP = filterToString(editMembers, 'Club Nbr', 8653)

        print('Getting JSON Data...')
        whJson = getJsonMemberTransactions(editWH, [listIDs['WH']])
        print(json.dumps(whJson, indent=2))
        bcJson = getJsonMemberTransactions(editBC, [listIDs['BC']])
        print(json.dumps(bcJson, indent=2))
        gmfJson = getJsonMemberTransactions(editGMF, [listIDs['GMF']])
        print(json.dumps(gmfJson, indent=2))
        pJson = getJsonMemberTransactions(editP, [listIDs['P']])
        print(json.dumps(pJson, indent=2))

        print('Sending Transactions to CC...')
        bulkImport(authData, gmfJson, whJson, pJson, bcJson)
        print('Done!')

        #os.remove(subjects['Newly Created'])
        #os.remove(subjects['Member Transactions'])
