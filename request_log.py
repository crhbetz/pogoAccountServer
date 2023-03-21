import logging
import pickle
import os
import sys

from collections import deque, UserDict
from loguru import logger
from operator import itemgetter

from config import Config


#logger = logging.getLogger(__name__)


class RequestLog(UserDict):
    def __init__(self, *args, filename=".request_log.pickle"):
        super().__init__(*args)
        self.config = Config()
        self.filename = filename
        self.__load_pickle_data()
        if not self.data:
            self.data: dict = {}
        logger.trace(f"Loaded request log data: {self.data}")

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__save_pickle_data()

    def __save_pickle_data(self):
        try:
            with open("{}/{}".format(os.path.dirname(os.path.abspath(__file__)), self.filename), "wb") as datafile:
                pickle.dump(self.data, datafile, -1)
                logger.trace("Saved data to pickle file")
                return True
        except Exception as e:
            logger.warning("Failed saving to pickle file: {}".format(e))
            return False

    def __load_pickle_data(self):
        try:
            with open("{}/{}".format(os.path.dirname(os.path.abspath(__file__)), self.filename), "rb") as datafile:
                data = pickle.load(datafile)
                self.data = data
                logger.info("request log data loaded from pickle file")
        except Exception as e:
            logger.warning("exception trying to load pickle'd data: {}".format(e))
            return None

    def __create_if_not_exists(self, name):
        if not name in self.data:
            self.data[name] = deque(maxlen=self.config.rate_limit_number)

    def log(self, name, request):
        self.__create_if_not_exists(name)
        self.data[name].append(request)
        self.__save_pickle_data()
        return True

    def rotate(self, device):
        try:
            self.data[device].rotate(-1)
            self.__save_pickle_data()
            return True
        except Exception as e:
            logger.warning(f"Exception trying to rotate deque of {device}: {e}")
            return False

    def get_logged_usernames(self, device):
        return map(itemgetter('username'), self.data[device]) if device in self.data else []
