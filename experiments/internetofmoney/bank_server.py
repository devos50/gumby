import json

import time

import logging
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
from twisted.web import resource
from twisted.web import server


class BankServer(object):
    """
    This centralized bank server keeps tracks of outstanding balances/transactions etc for ibans.
    """

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.bank_accounts = {}

    def start(self):
        self._logger.info("Starting bank server")
        self.root_endpoint = RootEndpoint(self)
        site = server.Site(resource=self.root_endpoint)
        self.site = reactor.listenTCP(8085, site)

    def stop(self):
        self._logger.info("Stopping bank server")
        return maybeDeferred(self.site.stopListening)

    def write_stats(self):
        with open('bank_balances.log', 'w', 0) as balances_file:
            balances_file.write('iban,balance\n')
            for iban, account in self.bank_accounts.iteritems():
                balances_file.write('%s,%f\n' % (iban, account["balance"]))

        all_transactions = []
        for iban, account in self.bank_accounts.iteritems():
            all_transactions.extend(account["transactions"])

        sorted_transactions = sorted(all_transactions, key=lambda tx: tx["timestamp"])
        with open('bank_transactions.log', 'w', 0) as transactions_file:
            transactions_file.write('from,to,amount,description,timestamp\n')
            for tx in sorted_transactions:
                transactions_file.write('%s,%s,%.2f,%s,%f\n' %
                                        (tx["from"], tx["to"], tx["amount"], tx["description"], tx["timestamp"]))


class RootEndpoint(resource.Resource):

    def __init__(self, bank_server):
        resource.Resource.__init__(self)
        self.putChild("register", RegisterEndpoint(bank_server))
        self.putChild("balance", BalanceEndpoint(bank_server))
        self.putChild("transactions", TransactionsEndpoint(bank_server))
        self.putChild("payment", PaymentEndpoint(bank_server))


class RegisterEndpoint(resource.Resource):

    def __init__(self, bank_server):
        resource.Resource.__init__(self)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.bank_server = bank_server

    def render_GET(self, request):
        self._logger.info("Received request for registration with iban %s", request.args['iban'][0])
        self.bank_server.bank_accounts[request.args['iban'][0]] = {'balance': 100000, 'transactions': []}
        return json.dumps({"success": True})


class BalanceEndpoint(resource.Resource):

    def __init__(self, bank_server):
        resource.Resource.__init__(self)
        self.bank_server = bank_server

    def render_GET(self, request):
        balance = self.bank_server.bank_accounts[request.args['iban'][0]]['balance']
        return json.dumps({"available": balance, "pending": 0.0, "currency": "EUR"})


class TransactionsEndpoint(resource.Resource):

    def __init__(self, bank_server):
        resource.Resource.__init__(self)
        self.bank_server = bank_server

    def render_GET(self, request):
        return json.dumps(self.bank_server.bank_accounts[request.args['iban'][0]]['transactions'])


class PaymentEndpoint(resource.Resource):

    def __init__(self, bank_server):
        resource.Resource.__init__(self)
        self.bank_server = bank_server

    def render_GET(self, request):
        from_iban = request.args['from'][0]
        to_iban = request.args['to'][0]
        amount = float(request.args['amount'][0])
        description = request.args['description'][0]

        # Subtract balance and create new transaction
        self.bank_server.bank_accounts[from_iban]["balance"] -= amount
        new_tx = {
            'id': str(),
            'outgoing': True,
            'from': from_iban,
            'to': to_iban,
            'amount': amount,
            'fee_amount': 0,
            'currency': 'EUR',
            'timestamp': time.time(),
            'description': description
        }
        self.bank_server.bank_accounts[from_iban]["transactions"].append(new_tx)

        # TODO add delay
        if to_iban not in self.bank_server.bank_accounts:  # Create a new account
            self.bank_server.bank_accounts[to_iban] = {'balance': 100000, 'transactions': []}

        self.bank_server.bank_accounts[to_iban]["balance"] += amount
        incoming_tx = {
            'id': str(),
            'outgoing': False,
            'from': from_iban,
            'to': to_iban,
            'amount': amount,
            'fee_amount': 0,
            'currency': 'EUR',
            'timestamp': time.time(),
            'description': description
        }
        self.bank_server.bank_accounts[to_iban]["transactions"].append(incoming_tx)

        return json.dumps({"success": True})
