from ondilo import Ondilo
import webbrowser

client = Ondilo(redirect_uri="https://example.com/api")
print('')
print('')
print('After you\'ll press enter, The ondilo portal will be opened in your default browser. Authenticate yourself and copy the resulting url here')
print('')
input('Press enter to continue')
webbrowser.open(client.get_authurl())

print('')
redirect_response = input('Paste the full redirect URL here:')
myToken =client.request_token(authorization_response=redirect_response)

print('')
print ("this is the token that you should copy in AWS Secrets manager: \n\n{}\n\n".format(myToken))
