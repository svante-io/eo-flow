import logging
import sys

LEVEL = logging.INFO
logging.basicConfig(stream=sys.stdout, level=LEVEL)
logger = logging.getLogger(__name__)


def list_loggers():
    rootlogger = logging.getLogger()
    print(rootlogger)
    for h in rootlogger.handlers:
        print("     %s" % h)

    for nm, lgr in logging.Logger.manager.loggerDict.items():
        print("+ [%-20s] %s " % (nm, lgr))
        if not isinstance(lgr, logging.PlaceHolder):
            for h in lgr.handlers:
                print("     %s" % h)
