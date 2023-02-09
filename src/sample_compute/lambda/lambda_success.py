""" Sample Lambda to simulate success """
import logging


def handler(event, context):
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    log.info("Lambda success execution completed !")
