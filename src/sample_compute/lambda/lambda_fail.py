""" Sample Lambda to simulate failure """


def handler(event, context):
    raise NameError("Some serious exception ")
