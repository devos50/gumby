import json
from urllib import quote_plus

from internetofmoney.managers.BaseManager import BaseManager
from internetofmoney.address import Address
from twisted.internet.defer import fail, succeed
from twisted.python.failure import Failure


class DummyManager(BaseManager):
    """
    This manager uses a centralized banking server to query transactions and balance.
    """

    def __init__(self, database, cache_dir='cache'):
        super(DummyManager, self).__init__(database, cache_dir)
        self.logged_in = False
        self.bank_ip = None
        self.iban = None

    def persistent_storage_filename(self):
        return '%s.json' % self.get_bank_id()

    def is_logged_in(self):
        return self.logged_in

    def on_registered(self, _):
        self.persistent_storage['registered'] = True

        return self.login()  # Immediately login

    def register(self, iban):
        self.iban = iban
        self.database.log_event('info', 'Starting registration sequence for %s' % self.get_bank_name())

        # Perform a request to the central server
        return self.perform_request("http://%s:8085/register?iban=%s" % (self.bank_ip, self.iban)).addCallback(self.on_registered)

    def login(self):
        if not self.is_registered():
            return fail(Failure(RuntimeError("not registered")))

        self.database.log_event('info', 'Starting login sequence for %s' % self.get_bank_name())

        self.logged_in = True
        return succeed(None)

    def perform_balance_request(self):
        def on_balance_response(response):
            return json.loads(response)

        return self.perform_request("http://%s:8085/balance?iban=%s" % (self.bank_ip, self.iban))\
            .addCallback(on_balance_response)

    def get_balance(self):
        self.database.log_event('info', 'Fetching balance for %s, account %s' %
                                (self.get_bank_name(), self.get_address()))
        return self.check_login().addCallback(lambda _: self.perform_balance_request())

    def perform_payment_request(self, amount, destination_account, description):
        return self.perform_request("http://%s:8085/payment?from=%s&to=%s&amount=%f&description=%s" %
                                    (self.bank_ip, self.iban, destination_account, amount, quote_plus(description)))\
            .addCallback(lambda _: 'a' * 20)

    def perform_payment(self, amount, destination_account, description):
        self.database.log_event('info', 'Starting %s payment with amount %f to %s (description: %s)' %
                                (self.get_bank_name(), amount, destination_account, description))

        def on_balance(balance):
            if balance['available'] < float(amount):
                return fail(Failure(RuntimeError('Not enough balance!')))

            return self.perform_payment_request(amount, destination_account, description)

        return self.check_login().addCallback(lambda _: self.get_balance()).addCallback(on_balance)

    def on_transactions(self, transactions):
        return json.loads(transactions)

    def perform_get_transactions_request(self):
        return self.perform_request("http://%s:8085/transactions?iban=%s" % (self.bank_ip, self.iban))\
            .addCallback(self.on_transactions)

    def get_transactions(self):
        self.database.log_event('info', 'Fetching %s transactions of account %s' %
                                (self.get_bank_name(), self.get_address()))
        return self.check_login().addCallback(lambda _: self.perform_get_transactions_request())

    def get_address(self):
        return Address(self.get_bank_id(), self.iban)


class Dummy1Manager(DummyManager):

    def get_bank_name(self):
        return 'Dummy1'

    def get_bank_id(self):
        return 'DUMA'


class Dummy2Manager(DummyManager):

    def get_bank_name(self):
        return 'Dummy2'

    def get_bank_id(self):
        return 'DUMB'


class Dummy3Manager(DummyManager):

    def get_bank_name(self):
        return 'Dummy3'

    def get_bank_id(self):
        return 'DUMC'


class Dummy4Manager(DummyManager):

    def get_bank_name(self):
        return 'Dummy4'

    def get_bank_id(self):
        return 'DUMD'


class Dummy5Manager(DummyManager):

    def get_bank_name(self):
        return 'Dummy5'

    def get_bank_id(self):
        return 'DUME'
