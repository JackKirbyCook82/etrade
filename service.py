# -*- coding: utf-8 -*-
"""
Created on Thurs May 29 2025
@name:   ETrade Service Objects
@author: Jack Kirby Cook

"""

from webscraping.webreaders import WebService
from webscraping.webdrivers import WebDriver
from webscraping.webdatas import WebELMT

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeWebService"]
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


AUTHORIZE = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
REQUEST = "https://api.etrade.com/oauth/request_token"
ACCESS = "https://api.etrade.com/oauth/access_token"
BASE = "https://api.etrade.com"


class ETradeUsernameData(WebELMT.Input, locator=r"//input[@id='USER']", key="username"): pass
class ETradePasswordData(WebELMT.Input, locator=r"//input[@id='password']", key="password"): pass
class ETradeLoginData(WebELMT.Button, locator=r"//button[@id='mfaLogonButton']", key="login"): pass
class ETradeAcceptData(WebELMT.Button, locator=r"//input[@id='acceptSubmit']", key="accept"): pass
class ETradeSecurity(WebELMT.Value, locator=r"//input[@type='text']", key="security"): pass


class ETradeWebService(WebService, authorize=AUTHORIZE, request=REQUEST, access=ACCESS, base=BASE):
    def __init__(self, *args, executable, authorize, timeout=60, port=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__executable = executable
        self.__timeout = int(timeout)
        self.__authorize = authorize
        self.__port = port

    def security(self, url, *args, **kwargs):
        with WebDriver(executable=self.executable, delay=self.delay, timeout=self.timeout, port=self.port) as source:
            source.load(str(url), *args, **kwargs)
            username = ETradeUsernameData(source.elmt, *args, timeout=self.timeout, **kwargs)
            password = ETradePasswordData(source.elmt, *args, timeout=self.timeout, **kwargs)
            login = ETradeLoginData(source.elmt, *args, timeout=self.timeout, **kwargs)
            username.fill(self.authorize.username)
            password.fill(self.authorize.password)
            login.click()
            accept = ETradeAcceptData(source.elmt, *args, timeout=self.timeout, **kwargs)
            accept.click()
            security = ETradeSecurity(source.elmt, *args, timeout=self.timeout, **kwargs)
            return security(*args, **kwargs)

    @property
    def executable(self): return self.__executable
    @property
    def authorize(self): return self.__authorize
    @property
    def timeout(self): return self.__timeout
    @property
    def port(self): return self.__port



