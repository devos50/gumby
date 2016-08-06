import logging
from os import environ
from random import random
from twisted.internet import reactor
from gumby.log import setupLogging
from gumby.sync import ExperimentClient, ExperimentClientFactory


class TriblerVideoExperimentClient(ExperimentClient):

    def onIdReceived(self):
        reactor.stop()

if __name__ == '__main__':
    from gumby.instrumentation import init_instrumentation
    init_instrumentation()
    setupLogging()
    factory = ExperimentClientFactory({}, TriblerVideoExperimentClient)
    logger = logging.getLogger()
    logger.debug("Connecting to: %s:%s", environ['SYNC_HOST'], int(environ['SYNC_PORT']))
    # Wait for a random amount of time before connecting to try to not overload the server when we have a lot of connections
    reactor.callLater(random() * 10,
                      lambda: reactor.connectTCP(environ['SYNC_HOST'], int(environ['SYNC_PORT']), factory))
    reactor.exitCode = 0
    reactor.run()
    exit(reactor.exitCode)
