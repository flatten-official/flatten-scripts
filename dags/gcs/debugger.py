def enable_cloud_debugger():
    """https://cloud.google.com/debugger/docs/setup/python?hl=en_GB&_ga=2.68834001.-1991847693.1585366893"""
    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass