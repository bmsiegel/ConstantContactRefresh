from flask import Flask
from flask import request
import requests
import base64
import sys
app = Flask(__name__)

with open(sys.argv[1], 'r') as f:
	data = f.read().splitlines()
	cid = data[2]
	csec = data[3]
	print(cid, csec)
	
@app.route('/')
def a():
	code = request.args.get('code')
	credential = base64.b64encode('{}:{}'.format(cid, csec).encode('ascii')) 
	authData = {'Authorization':'Basic {}'.format(credential.decode())}
	print(authData)
	print(requests.post('https://idfed.constantcontact.com/as/token.oauth2?code={}&redirect_uri=http://localhost:8080&grant_type=authorization_code'.format(code), headers = authData).content)
	return code

@app.route('/auth')
def b():
	return 'abcd'	

if __name__ == '__main__':
	app.run(host='', port='8080')
