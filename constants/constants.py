# -*- coding: utf-8 -*-
"""
Requires Python 3.0 or later
"""

__author__ = "Jorge Morfinez Mojica (jorgemorfinez@ofix.mx)"
__copyright__ = "Copyright 2020, Jorge Morfinez Mojica"
__license__ = "Ofix S.A. de C.V."
__history__ = """ """
__version__ = "1.20.D10.1.2.3 ($Rev: 11 $)"

import yaml


class Constants:

    @staticmethod
    def get_constants_file(self):
        try:
            with open(self, 'r') as ymlfile:
                cfg = yaml.safe_load(ymlfile)

        except yaml.YAMLError as exc:
            print("Error in configuration file:", exc)
        except yaml.MarkedYAMLError as mye:
            raise yaml.ImproperlyConfigured(
                'Your settings file(s) contain invalid YAML syntax! Please fix and restart!, {}'.format(str(mye))
            )

        return cfg

