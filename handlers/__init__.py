def get_handler(name):
    from importlib import import_module
    return import_module('{}.{}'.format(__name__, name), package=__name__)
