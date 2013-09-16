#!/usr/bin/env python2

class TooManyRetries(Exception):
    pass

def with_max_retries(max_retries, exceptions_to_catch):
    """
    Call a function until it doesn't raise an expected exception, with a
    limited number of retries
    """
    def decorator(f):
        def decorated(*args, **kwargs):
            def repeat(retries_left):
                try:
                    return f(*args, **kwargs)
                except exceptions_to_catch:
                    if retries_left > 0:
                        repeat(retries_left - 1)
                    else:
                        raise TooManyRetries()
            return repeat(max_retries)
        return decorated
    return decorator

if __name__ == '__main__':
    # Tests

    MAX_RETRIES = 5

    @with_max_retries(MAX_RETRIES, Exception)
    def raise_n_times():
        global raises_left
        if raises_left == 0:
            return
        else:
            raises_left -= 1
            print('-- raises_left: %d' % raises_left)
            raise Exception

    print('First call')

    raises_left = 5
    try:
        raise_n_times()
    except:
        raise AssertionError

    print('Second call')

    raises_left = 6
    try:
        raise_n_times()
    except:
        pass
    else:
        raise AssertionError

    print('Tests passed')
