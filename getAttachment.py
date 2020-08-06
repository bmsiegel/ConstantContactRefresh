import imaplib
import base64
import os
import email

def getAttachment(keyword):
	with open('credentials.txt', 'r') as cred:
		lines = cred.read().splitlines()
	
	email_user = lines[0]
	password = lines[1]
	
	mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
	mail.login(email_user, password)
	
	mail.select('GMF')

	try:
		type, data = mail.search(None, 'SUBJECT', '"{}"'.format(keyword))
		mail_ids = data[0]
		id_list = mail_ids.split()
		#get most recent email from GMF Label
		email_bytes = mail.fetch(id_list[0], '(RFC822)')[1][0][1]
		raw_email = email_bytes.decode('utf-8')
	except:
		return False
	
	for part in email.message_from_string(raw_email).walk():
		if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
			continue
		fileName = part.get_filename()

		if fileName:
			with open('member_export.csv', 'wb') as f:
				f.write(part.get_payload(decode=True))
				f.close()
		else:
			print('No Attachment in Email...')
			return False
	mail.store(mail_ids, '+FLAGS', '\\Deleted')
	mail.expunge()
	return os.path.exists('member_export.csv')
