# -*- coding: utf-8 -*-
"""
Created on Thurs May 29 2025
@name:   ETrade Service Objects
@author: Jack Kirby Cook

"""

from webscraping.webreaders import WebService
from webscraping.webdrivers import WebDriver
from webscraping.webpages import WebELMTPage
from webscraping.webdatas import WebELMT

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeServiceReader"]
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


class ETradeServicePage(WebELMTPage):
    def execute(self, url, *args, authorize, timeout, **kwargs):
        self.load(*args, url=str(url), **kwargs)
        username = ETradeUsernameData(self.elmt, *args, timeout=timeout, **kwargs)
        password = ETradePasswordData(self.elmt, *args, timeout=timeout, **kwargs)
        login = ETradeLoginData(self.elmt, *args, timeout=timeout, **kwargs)
        username.fill(authorize.username)
        password.fill(authorize.password)
        with self.delayer: login.click()
        accept = ETradeAcceptData(self.elmt, *args, timeout=timeout, **kwargs)
        with self.delayer: accept.click()
        security = ETradeSecurity(self.elmt, *args, timeout=timeout, **kwargs)
        return security(*args, **kwargs)


class ETradeServiceReader(WebService, authorize=AUTHORIZE, request=REQUEST, access=ACCESS, base=BASE):
    def __init__(self, *args, executable, authorize, delay, timeout=60, port=None, **kwargs):
        super().__init__(*args, delay=delay, **kwargs)
        self.__executable = executable
        self.__timeout = int(timeout)
        self.__authorize = authorize
        self.__delay = delay
        self.__port = port

    def security(self, url, *args, **kwargs):
        with WebDriver(executable=self.executable, timeout=self.timeout, delay=self.delay, port=self.port) as source:
            page = ETradeServicePage(*args, source=source, **kwargs)
            security = page(url, *args, timeout=self.timeout, authorize=self.authorize, **kwargs)
            return security

    @property
    def executable(self): return self.__executable
    @property
    def authorize(self): return self.__authorize
    @property
    def timeout(self): return self.__timeout
    @property
    def delay(self): return self.__delay
    @property
    def port(self): return self.__port



