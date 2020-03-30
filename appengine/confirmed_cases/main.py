import datetime

from flask import Flask, Response, request
from service import main
import os

app = Flask(__name__)


@app.route('/')
def root():
    # only trigger the service when the cron job is run
    if not ('X-Appengine-Cron' in request.headers and request.headers['X-Appengine-Cron'] == 'true'):
        return Response(status=403)
    try:
        main()
        return Response(status=200)
    except Exception as e:
        print(e)
        return Response(status=500)


# for local testing
if __name__ == '__main__':
    # Using debug as true is a security vulnerability in production.
    # Anything other than true will result in debug as false.
    # https://flask.palletsprojects.com/en/1.1.x/quickstart/#a-minimal-application
    debug = os.environ['debug'].lower() == "true"

    app.run(host='127.0.0.1', port=8080, debug=debug)