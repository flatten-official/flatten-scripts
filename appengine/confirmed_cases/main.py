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


def enable_cloud_debugger():
    """https://cloud.google.com/debugger/docs/setup/python?hl=en_GB&_ga=2.68834001.-1991847693.1585366893"""
    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass


enable_cloud_debugger()

# Only called when testing locally
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
