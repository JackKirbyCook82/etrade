# -*- coding: utf-8 -*-
"""
Created on Thurs May 29 2025
@name:   ETrade Service Objects
@author: Jack Kirby Cook

"""

from webscraping.webservices import WebService
from webscraping.webreaders import WebReader
from webscraping.webdrivers import WebDriver
from webscraping.webdatas import WebELMT

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


AUTHORIZE = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
REQUEST = "https://api.etrade.com/oauth/request_token"
ACCESS = "https://api.etrade.com/oauth/access_token"
BASE = "https://api.etrade.com"


class ETradeUsernameData(WebELMT.Input, locator=r"", key="username"): pass
class ETradePasswordData(WebELMT.Input, locator=r"", key="password"): pass
class ETradeLoginData(WebELMT.Button, locator=r"", key="login"): pass
class ETradeSecurity(WebELMT.Text, locator=r"", key="security"): pass


class ETradeWebService(WebService, WebReader, authorize=AUTHORIZE, request=REQUEST, access=ACCESS, base=BASE):
    def __init__(self, *args, executable, authorize, port=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__executable = executable
        self.__authorize = authorize
        self.__port = port

    def start(self): self.session = self.service()
    def load(self, url, *args, **kwargs):
        parameters = {"header_auth": True}
        self.console(str(url), title="Loading")
        super().load(url, *args, parameters=parameters, **kwargs)

    def security(self, url, *args, **kwargs):
        with WebDriver(executable=self.executable, delay=self.delay, port=self.port) as source:
            source.load(str(url), *args, **kwargs)
            username = ETradeUsernameData(source.elmt, *args, **kwargs)
            password = ETradePasswordData(source.elmt, *args, **kwargs)
            login = ETradeLoginData(source.elmt, *args, **kwargs)
            username.fill(self.authorize.username)
            password.fill(self.authorize.password)
            login.click()
            security = ETradeSecurity(source.elmt, *args, **kwargs)
            return security(*args, **kwargs)

    @property
    def executable(self): return self.__executable
    @property
    def authorize(self): return self.__authorize
    @property
    def port(self): return self.__port


