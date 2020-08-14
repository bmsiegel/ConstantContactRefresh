import requests
import os
import base64
import json
import pandas as pd
import time
from datetime import datetime
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

def getJson(csv, lists):
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

def getClub(newMembers, clubCode):
	return newMembers[newMembers['Club Nbr'] == clubCode].to_csv(index=False)

if __name__ == '__main__':

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
	newWH = getClub(newMembers, 8656)
	newBC = getClub(newMembers, 8655)
	newGMF = getClub(newMembers, 8070)
	newP = getClub(newMembers, 8653)
	
	print('Getting JSON Data...')
	whJson = getJson(newWH, [listIDs['WH']])
	bcJson = getJson(newBC, [listIDs['BC']])
	gmfJson = getJson(newGMF, [listIDs['GMF']])
	pJson = getJson(newP, [listIDs['P']])
	
	print('Sending New Members to CC...')
	r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=whJson)
	time.sleep(1)
	r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=bcJson)
	time.sleep(1)
	r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=gmfJson)
	time.sleep(1)
	r = requests.post('https://api.cc.email/v3/activities/contacts_json_import', headers=authData, json=pJson)
	time.sleep(1)
	print('Done!')	
	
	editMembers = pd.read_csv('Member Transactions.csv')

	os.remove(subjects['Newly Created'])
	os.remove(subjects['Member Transactions'])
