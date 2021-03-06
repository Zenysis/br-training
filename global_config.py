# Flask
DEFAULT_SECRET_KEY = 'abc123'

# Mailgun settings
MAILGUN_API_KEY = 'key-xxx'
MAILGUN_NAME = 'mg.hostname.com'
MAILGUN_SENDER = 'noreply@mg.hostname.com'

# Druid settings
DEFAULT_DRUID_HOST = 'http://br-demo-druid.corp.clambda.com'

# Phabricator settings
PASSPHRASE_ENDPOINT = 'https://phab.hostname.com/api/passphrase.query'

# Mail
NOREPLY_EMAIL = 'noreply@mg.hostname.com'
SUPPORT_EMAIL = 'support@hostname.com'
RENDERBOT_EMAIL = 'render_bot@hostname.com'

DATA_UPLOAD_DEFAULT_NOTIFY = ['foo@hostname.com', 'bar@hostname.com']

# In order to read and write from google sheet you will need proper authorization.
# https://developers.google.com/identity/protocols/oauth2
GOOGLE_SERVICE_SECRET_CREDENTIAL = ''

POSTGRES_DB_URI = 'postgresql://username:password@hostname/db-name'

REDIS_HOST = 'redis'
