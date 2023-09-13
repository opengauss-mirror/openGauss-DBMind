import os
import sys

try:
    from dbmind.components.knob_estimator import main
except ImportError:
    libpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
    sys.path.append(libpath)
    from knob_estimator.main import main

main(sys.argv[1:])
