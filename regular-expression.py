import re


msg = 'This is, a test, of the emergency broadcast system.'
my_string = re.compile(r'\W+', re.UNICODE).split(msg.lower())
print (my_string)