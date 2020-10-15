import logging

__module_logger__ = None


def set_logger(filename, mode='a', level='DEBUG'):
    global __module_logger__
    __module_logger__ = logging.getLogger(filename)
    __module_logger__.setLevel(level)

    if level == 'INFO':
        log_level = logging.INFO
    elif level == 'ERROR':
        log_level = logging.ERROR
    else:
        log_level = logging.DEBUG

    # log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # default level for log-file
    fh = logging.FileHandler(filename, mode=mode)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    __module_logger__.addHandler(fh)

    # ERROR
    if log_level != logging.ERROR:
        sh = logging.StreamHandler()
        sh.setLevel(logging.ERROR)
        sh.setFormatter(formatter)
        __module_logger__.addHandler(sh)


def log(message, level='DEBUG'):
    if __module_logger__ is None:
        raise

    if level == 'INFO':
        __module_logger__.info(message)
    elif level == 'ERROR':
        __module_logger__.error(message)
    elif level == 'DEBUG':
        __module_logger__.debug(message)
    else:
        __module_logger__.error(f'Wrong method {message}')


if __name__ == '__main__':
    print('Do nothing')