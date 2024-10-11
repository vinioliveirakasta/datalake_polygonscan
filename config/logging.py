level = None
config = {'version': 1,
          'formatters': {'simple': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}},
          'handlers': {'console':
                       {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'simple', 'stream': 'ext://sys.stdout'}},
          'loggers': {'simpleExample': {'level': 'INFO', 'handlers': ['console'], 'propagate': False}},
          'root': {'level': 'DEBUG', 'handlers': ['console']}}


def set_logging_config(formatters=None, handlers=None, loggers=None, root_level='INFO'):
    global config
    global level
    level = root_level

    config['root']['level'] = level


if __name__ == '__main__':
    set_logging_config(root_level='DEBUG')
    print(config)