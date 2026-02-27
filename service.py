# -*- coding: utf-8 -*-
"""
Created on Thurs May 29 2025
@name:   ETrade Service Objects
@author: Jack Kirby Cook

"""

import webbrowser
import tkinter as tk
from abc import ABC

from tornado.web import authenticated
from webscraping.webreaders import WebService
from webscraping.webdrivers import WebDriver
from webscraping.webpages import WebELMTPage
from webscraping.webdatas import WebELMT

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeDriverService", "ETradePromptService"]
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
class ETradeSecurityData(WebELMT.Value, locator=r"//input[@type='text']", key="security"): pass


class ETradeDriverPage(WebELMTPage):
    def execute(self, url, *args, timeout, **kwargs):
        self.load(*args, url=str(url), **kwargs)
        username = ETradeUsernameData(self.elmt, *args, timeout=timeout, **kwargs)
        password = ETradePasswordData(self.elmt, *args, timeout=timeout, **kwargs)
        login = ETradeLoginData(self.elmt, *args, timeout=timeout, **kwargs)
        username.fill(self.account.username)
        password.fill(selff.account.password)
        with self.delayer: login.click()
        accept = ETradeAcceptData(self.elmt, *args, timeout=timeout, **kwargs)
        with self.delayer: accept.click()
        security = ETradeSecurityData(self.elmt, *args, timeout=timeout, **kwargs)
        return security(*args, **kwargs)


class ETradeService(WebService, ABC, authorize=AUTHORIZE, request=REQUEST, access=ACCESS, base=BASE): pass
class ETradeDriverService(ETradeService):
    def __init__(self, *args, executable, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        self.__executable = executable
        self.__timeout = int(timeout)

    def security(self, url, *args, account, authenticator, delayer, **kwargs):
        parameters = dict(executable=self.executable, delayer=delayer, timeout=self.timeout)
        with WebDriver(**parameters) as source:
            page = ETradeDriverPage(*args, source=source, **kwargs)
            parameters = dict(account=account, authenticator=authenticator, timeout=self.timeout)
            security = page(url, *args, **parameters, **kwargs)
            return security

    @property
    def executable(self): return self.__executable
    @property
    def timeout(self): return self.__timeout


class ETradePromptService(ETradeService):
    def security(self, url, *args, **kwargs):
        webbrowser.open(str(url))
        window = tk.Tk()
        window.title("Enter Security Code:")
        variable = tk.StringVar()
        entry = tk.Entry(window, width=50, justify=tk.CENTER, textvariable=variable)
        entry.focus_set()
        entry.grid(padx=10, pady=10)
        button = tk.Button(window, text="Submit", command=window.destroy)
        button.grid(row=0, column=1, padx=10, pady=10)
        window.mainloop()
        return str(variable.get())




