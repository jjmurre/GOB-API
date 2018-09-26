import random
import string


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def random_array(callable=random_string):
    result = []
    for i in range(0, random.randint(1, 4)):
        result.append(callable())
    return result


def random_dict(callable=random_string):
    return {key: callable for key in random_array(random_string)}