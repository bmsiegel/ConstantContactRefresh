import requests
import os
import base64
import json
import math
import pandas as pd
import numpy as np
import time
import sys
from datetime import date, datetime
from getAttachment import getAttachment


def getBasicAuth():
    print('Reading Credentials...')
    with open(sys.argv[1], 'r') as f:
        data = f.read().splitlines()
        client_id = data[2]
        client_secret = data[3]
        refresh_token = data[4]
    credential = base64.b64encode('{}:{}'.format(
        client_id, client_secret).encode('ascii'))
    authData = {'Authorization': 'Basic {}'.format(credential.decode())}
    r = requests.post(
        'https://idfed.constantcontact.com/as/token.oauth2?refresh_token={}&grant_type=refresh_token'.format(refresh_token), headers=authData)
    authResponse = json.loads(r.content.decode())
    data[4] = authResponse['refresh_token']
    print('Updating Refresh Token...')
    with open(sys.argv[1], 'w') as f:
        for d in data:
            f.write(d + '\n')
    return authResponse['access_token']


def getNewlyCreatedJSON(row):
    try:
        birthDate = datetime.strptime(row['Birth Date'], '%m/%d/%Y')
    except:
        birthDate = None
    
    if pd.isnull(row['Email']):
        with open('bad.txt', 'a') as f:
            f.write('{} {}\n'.format(row['Member Name (first)'], row['Member Name (last)']))

    member = {
        'email': 'no_email_{}@fail.com'.format(time.time()) if pd.isnull(row['Email']) else row['Email'],
        'first_name': row['Member Name (first)'],
        'last_name': row['Member Name (last)'],
        'birthday_month': birthDate.month if birthDate else '',  # getMonth
        'birthday_day': birthDate.day if birthDate else '',  # getDay
        'company_name': row['Club Name'],
        'home_phone': row['Primary Phone'] if row['Cell Phone'] == '' else row['Primary Phone']
    }

    return member


def getMemberTransactionJSON(row):
    name = row['Member Name (last, first)']
    nameSplit = name.split(', ')
    lastName = nameSplit[0]
    firstName = nameSplit[1].split(' ')[0]
    email = row['Email']
    return {
        'email': email,
        'first_name': firstName,
        'last_name': lastName
    }


def getJson(csv, lists, jsonFunc):
    result = dict()
    result['import_data'] = list()
    result['list_ids'] = lists
    csv = csv.apply(jsonFunc, axis=1)
    if isinstance(csv, pd.core.series.Series):
        for val in csv:
            result['import_data'].append(val)
    return result


def filterTable(table, column, target):
    return table[table[column] == target]


def filterToString(table, column, target):
    return table[table[column] == target].to_csv(index=False)


def bulkImport(authData, sendList):
    for s in sendList:
        r = requests.post(
            'https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=s)
        time.sleep(1)


if __name__ == '__main__':

    print('{}'.format(date.today()))

    goOn = False

    subjects = {
        'Newly Created': 'newly_created.csv',
        'Member Transactions': 'member_transactions.csv'
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
    authData = {'Authorization': 'Bearer {}'.format(auth)}
    token = {'token': auth}

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
        elif l['name'] == 'Parisi Speed School':
            listIDs['P'] = l['list_id']
        elif l['name'] == 'Fifth Quarter Training':
            listIDs['BC'] = l['list_id']
        elif l['name'] == 'Wheel House Elite Cycle Studio':
            listIDs['WH'] = l['list_id']
        elif l['name'] == 'Brazilian Jiu Jitsu':
            listIDs['BJJ'] = l['list_id']
        elif l['name'] == 'CrossFit Garwood':
            listIDs['CF'] = l['list_id']
        elif l['name'] == 'Parisi ECC':
            listIDs['ECC'] = l['list_id']
        elif l['name'] == 'Gold Medal Fitness Cranford':
            listIDs['GMFCF'] = l['list_id']

    print(listIDs)

    print('Splitting New Members into Different Clubs...')
    try:
        newMembers = pd.read_csv(subjects['Newly Created'], dtype=str)
        newWH = filterTable(newMembers, 'Club Nbr', '8656')
        newBC = filterTable(newMembers, 'Club Nbr', '8655')
        newP = filterTable(newMembers, 'Club Nbr', '8653')
        newBJJ = filterTable(newMembers, 'Club Nbr', '8099')
        newCF = filterTable(newMembers, 'Club Nbr', '8654')
        newECC = filterTable(newMembers, 'Club Nbr', '8057')
        newGMFCF = filterTable(newMembers, 'Club Nbr', '6616')

        print('Getting JSON Data...')
        whJson = getJson(newWH, [listIDs['WH']], getNewlyCreatedJSON)
        print(json.dumps(whJson, indent=2))
        bcJson = getJson(newBC, [listIDs['BC']], getNewlyCreatedJSON)
        print(json.dumps(bcJson, indent=2))
        gmfJson = getJson(newMembers, [listIDs['GMF']], getNewlyCreatedJSON)
        print(json.dumps(gmfJson, indent=2))
        pJson = getJson(newP, [listIDs['P']], getNewlyCreatedJSON)
        print(json.dumps(pJson, indent=2))
        BJJJson = getJson(newBJJ, [listIDs['BJJ']], getNewlyCreatedJSON)
        print(json.dumps(BJJJson, indent=2))
        CFJson = getJson(newCF, [listIDs['CF']], getNewlyCreatedJSON)
        print(json.dumps(CFJson, indent=2))
        ECCJson = getJson(newECC, [listIDs['ECC']], getNewlyCreatedJSON)
        print(json.dumps(ECCJson, indent=2))
        GMFCFJson = getJson(newGMFCF, [listIDs['GMFCF']], getNewlyCreatedJSON)
        print(json.dumps(GMFCFJson, indent=2))

        print('Sending New Members to CC...')
        sendList = [whJson, bcJson, gmfJson, pJson,
                    BJJJson, CFJson, ECCJson, GMFCFJson]
        bulkImport(authData, sendList)
    except:
        print('No New Members!')

    try:
        editMembers = pd.read_csv(subjects['Member Transactions'], dtype=str)
        print('Filtering Demographic Changes...')
        editMembers = filterTable(
            editMembers, 'Change Type', 'Update Demographics')
        print('Getting Member Transactions into Different Clubs...')
        editWH = filterTable(editMembers, 'Club Nbr', '8656')
        editBC = filterTable(editMembers, 'Club Nbr', '8655')
        editP = filterTable(editMembers, 'Club Nbr', '8653')
        editBJJ = filterTable(editMembers, 'Club Nbr', '8099')
        editCF = filterTable(editMembers, 'Club Nbr', '8654')
        editECC = filterTable(editMembers, 'Club Nbr', '8057')
        editGMFCF = filterTable(editMembers, 'Club Nbr', '6616')

        print('Getting JSON Data...')
        whJson = getJson(editWH, [listIDs['WH']], getMemberTransactionJSON)
        print(json.dumps(whJson, indent=2))
        bcJson = getJson(editBC, [listIDs['BC']], getMemberTransactionJSON)
        print(json.dumps(bcJson, indent=2))
        gmfJson = getJson(editMembers, [listIDs['GMF']], getMemberTransactionJSON)
        print(json.dumps(gmfJson, indent=2))
        pJson = getJson(editP, [listIDs['P']], getMemberTransactionJSON)
        print(json.dumps(pJson, indent=2))
        BJJJson = getJson(editBJJ, [listIDs['BJJ']], getMemberTransactionJSON)
        print(json.dumps(BJJJson, indent=2))
        CFJson = getJson(editCF, [listIDs['CF']], getMemberTransactionJSON)
        print(json.dumps(CFJson, indent=2))
        ECCJson = getJson(editECC, [listIDs['ECC']], getMemberTransactionJSON)
        print(json.dumps(ECCJson, indent=2))
        GMFCFJson = getJson(
            editGMFCF, [listIDs['GMFCF']], getMemberTransactionJSON)
        print(json.dumps(GMFCFJson, indent=2))

        print('Sending Transactions to CC...')
        sendList = [whJson, bcJson, gmfJson, pJson,
                    BJJJson, CFJson, ECCJson, GMFCFJson]
        bulkImport(authData, sendList)
    except:
        print('No Member Transactions!')

    print('Done!')
