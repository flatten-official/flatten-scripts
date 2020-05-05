from utils.config import detect_project, load_config


def enable_cloud_debugger():
    """
    https://cloud.google.com/debugger/docs/setup/python
    If we're on the staging environment enable the cloud debugger if available
    """
    if detect_project(load_config()) == "staging":
        try:
            import googleclouddebugger
            googleclouddebugger.enable()
        except ImportError:
            pass
