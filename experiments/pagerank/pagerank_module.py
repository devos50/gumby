import os
import random

import networkx as nx

from twisted.internet import reactor

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


def pagerank_scipy_patched(G, alpha=0.85, personalization=None,
                           max_iter=100, tol=1.0e-6, weight='weight',
                           dangling=None):
    """Return the PageRank of the nodes in the graph.
    PageRank computes a ranking of the nodes in the graph G based on
    the structure of the incoming links. It was originally designed as
    an algorithm to rank web pages.
    Parameters
    ----------
    G : graph
      A NetworkX graph.  Undirected graphs will be converted to a directed
      graph with two directed edges for each undirected edge.
    alpha : float, optional
      Damping parameter for PageRank, default=0.85.
    personalization: dict, optional
       The "personalization vector" consisting of a dictionary with a
       key for every graph node and nonzero personalization value for each
       node. By default, a uniform distribution is used.
    max_iter : integer, optional
      Maximum number of iterations in power method eigenvalue solver.
    tol : float, optional
      Error tolerance used to check convergence in power method solver.
    weight : key, optional
      Edge data key to use as weight.  If None weights are set to 1.
    dangling: dict, optional
      The outedges to be assigned to any "dangling" nodes, i.e., nodes without
      any outedges. The dict key is the node the outedge points to and the dict
      value is the weight of that outedge. By default, dangling nodes are given
      outedges according to the personalization vector (uniform if not
      specified) This must be selected to result in an irreducible transition
      matrix (see notes under google_matrix). It may be common to have the
      dangling dict to be the same as the personalization dict.
    Returns
    -------
    pagerank : dictionary
       Dictionary of nodes with PageRank as value
    Examples
    --------
    >>> G = nx.DiGraph(nx.path_graph(4))
    >>> pr = nx.pagerank_scipy(G, alpha=0.9)
    Notes
    -----
    The eigenvector calculation uses power iteration with a SciPy
    sparse matrix representation.
    This implementation works with Multi(Di)Graphs. For multigraphs the
    weight between two nodes is set to be the sum of all edge weights
    between those nodes.
    See Also
    --------
    pagerank, pagerank_numpy, google_matrix
    References
    ----------
    .. [1] A. Langville and C. Meyer,
       "A survey of eigenvector methods of web information retrieval."
       http://citeseer.ist.psu.edu/713792.html
    .. [2] Page, Lawrence; Brin, Sergey; Motwani, Rajeev and Winograd, Terry,
       The PageRank citation ranking: Bringing order to the Web. 1999
       http://dbpubs.stanford.edu:8090/pub/showDoc.Fulltext?lang=en&doc=1999-66&format=pdf
    """
    import scipy.sparse

    N = len(G)
    if N == 0:
        return {}

    nodelist = G.nodes()
    M = nx.to_scipy_sparse_matrix(G, nodelist=nodelist, weight=weight,
                                  dtype=float)
    S = scipy.array(M.sum(axis=1)).flatten()
    S[S != 0] = 1.0 / S[S != 0]
    Q = scipy.sparse.spdiags(S.T, 0, *M.shape, format='csr')
    M = Q * M

    # initial vector
    x = scipy.repeat(1.0 / N, N)

    # Personalization vector
    if personalization is None:
        p = scipy.repeat(1.0 / N, N)
    else:
        p = scipy.array([personalization.get(n, 0) for n in nodelist],
                        dtype=float)
        p = p / p.sum()

    # Dangling nodes
    if dangling is None:
        dangling_weights = p
    else:
        missing = set(nodelist) - set(dangling)
        if missing:
            raise nx.NetworkXError('Dangling node dictionary '
                                   'must have a value for every node. '
                                   'Missing nodes %s' % missing)
        # Convert the dangling dictionary into an array in nodelist order
        dangling_weights = scipy.array([dangling[n] for n in nodelist],
                                       dtype=float)
        dangling_weights /= dangling_weights.sum()
    is_dangling = scipy.where(S == 0)[0]

    # power iteration: make up to max_iter iterations
    for _ in range(max_iter):
        xlast = x
        x = alpha * (x * M + sum(x[is_dangling]) * dangling_weights) + \
            (1 - alpha) * p
        # check convergence, l1 norm
        err = scipy.absolute(x - xlast).sum()
        if err < N * tol:
            return dict(zip(nodelist, map(float, x)))
    print(err)
    raise nx.NetworkXError('pagerank_scipy: power iteration failed to converge '
                           'in %d iterations.' % max_iter)


@static_module
class PagerankModule(ExperimentModule):

    def __init__(self, experiment):
        super(PagerankModule, self).__init__(experiment)

        random.seed(0)

        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "auctions_graph.txt")
        self.transactions = []

        with open(file_path) as tx_file:
            content = tx_file.readlines()
            for line in content:
                parts = line.split(',')
                self.transactions.append((parts[0], parts[1], float(parts[2]), int(parts[3])))

        self.transactions = sorted(self.transactions, key=lambda tup: tup[3])
        self.traders = set()
        for tx in self.transactions:
            self.traders.add(tx[0])
            self.traders.add(tx[1])

    def compute_temporal_pagerank(self, identity, transactions):
        last_seq_num = {}
        for tx in transactions:
            last_seq_num[tx[0]] = 1
            last_seq_num[tx[1]] = 1

        # Create temporal pagerank graph
        print "Creating graph..."
        G = nx.DiGraph()

        for tx in transactions:
            pubkey_requester = tx[0]
            pubkey_responder = tx[1]

            sequence_number_requester = last_seq_num[tx[0]]
            sequence_number_responder = last_seq_num[tx[1]]

            # In our market, we consider the amount of Bitcoin that have been transferred from A -> B.
            # For now, we assume that the value from B -> A is of equal worth.

            value_exchange = tx[2]

            G.add_edge((pubkey_requester, sequence_number_requester), (pubkey_requester, sequence_number_requester + 1),
                       contribution=value_exchange)
            G.add_edge((pubkey_requester, sequence_number_requester), (pubkey_responder, sequence_number_responder + 1),
                       contribution=value_exchange)

            G.add_edge((pubkey_responder, sequence_number_responder), (pubkey_responder, sequence_number_responder + 1),
                       contribution=value_exchange)
            G.add_edge((pubkey_responder, sequence_number_responder), (pubkey_requester, sequence_number_requester + 1),
                       contribution=value_exchange)

            last_seq_num[tx[0]] += 1
            last_seq_num[tx[1]] += 1

        print "Building personalization vector..."
        personal_nodes = [node1 for node1 in G.nodes() if node1[0] == identity]
        number_of_nodes = len(personal_nodes)
        personalisation = dict(zip(personal_nodes, [1.0 / number_of_nodes] * number_of_nodes))

        try:
            print "Performing PageRank..."
            result = pagerank_scipy_patched(G, personalization=personalisation, weight='contribution')
        except nx.NetworkXException:
            result = {}
            print("Empty Temporal PageRank, returning empty scores")

        sums = {}
        for interaction in result.keys():
            sums[interaction[0]] = sums.get(interaction[0], 0) + result[interaction]

        reputations = []
        for node_name, rep in sums.iteritems():
            reputations.append((node_name, rep))

        reputations = sorted(reputations, key=lambda tup: tup[1], reverse=True)
        return [node_name for node_name, _ in reputations], sums

    def compute_pagerank(self, identity, transactions, personalise=True):
        print "Building personal pagerank graph..."
        G = nx.Graph()

        for tx in transactions:
            G.add_edge(tx[0], tx[1], contribution=tx[2])

        if personalise:
            personalisation = {name: 1 if name == identity else 0 for name in G.nodes()}
            reputation = nx.pagerank_scipy(G, personalization=personalisation, weight='contribution')
        else:
            reputation = nx.pagerank_scipy(G, weight='contribution')

        reputations = []
        for node_name, rep in reputation.iteritems():
            reputations.append((node_name, rep))

        reputations = sorted(reputations, key=lambda tup: tup[1], reverse=True)
        return [node_name for node_name, _ in reputations], reputation

    def perform_sybil_attack(self, num_compromised_nodes, sybil_region_size):
        print "Performing Sybil Attack..."

        # Determine nodes
        nodes = set()
        earliest_transaction = {}

        for tx in self.transactions:
            nodes.add(tx[0])
            nodes.add(tx[1])

            # Determine earliest transactions
            if tx[0] not in earliest_transaction:
                earliest_transaction[tx[0]] = tx[3]
            if tx[1] not in earliest_transaction:
                earliest_transaction[tx[1]] = tx[3]

        sybil_transactions = [tx for tx in self.transactions]

        # Sybil Attack - 1) compromise random nodes
        compromised_nodes = random.sample(nodes, num_compromised_nodes)

        # Sybil Attack - 2) each compromised node creates a scale-free network with 50 additional identities
        sybil_ind = 1
        for compromised_node in compromised_nodes:
            for i in xrange(sybil_region_size):
                sybil_transactions.append((compromised_node, "sybil_%d_%d" % (sybil_ind, i), 1000000,
                                           earliest_transaction[compromised_node] - 1))
            sybil_ind += 1

        # Sort again after adding sybil transactions
        sybil_transactions = sorted(sybil_transactions, key=lambda tup: tup[3])

        return compromised_nodes, sybil_transactions

    def compute_diff(self, sybil_region_size, identity, algorithm):
        print "Computing diff for sybil region %d, %s" % (sybil_region_size, algorithm)

        # Determine original scores (no Sybils)
        print "Computing original scores..."
        if algorithm == 'pagerank':
            original_ranking, original_reps = self.compute_pagerank(identity, self.transactions, personalise=False)
        elif algorithm == 'personalised_pagerank':
            original_ranking, original_reps = self.compute_pagerank(identity, self.transactions)
        else:
            original_ranking, original_reps = self.compute_temporal_pagerank(identity, self.transactions)

        compromised_nodes, transactions = self.perform_sybil_attack(100, sybil_region_size)
        print "Computing scores after Sybil Attack..."

        if algorithm == 'pagerank':
            sybil_ranking, sybil_reps = self.compute_pagerank(identity, transactions, personalise=False)
        elif algorithm == 'personalised_pagerank':
            sybil_ranking, sybil_reps = self.compute_pagerank(identity, transactions)
        else:
            sybil_ranking, sybil_reps = self.compute_temporal_pagerank(identity, transactions)

        # Determine value diffs
        total_diff_values = 0
        for compromised_node in compromised_nodes:
            total_diff_values += float(sybil_reps[compromised_node]) / float(original_reps[compromised_node])

        # Determine ranking diffs
        total_diff_percentage = 0
        for compromised_node in compromised_nodes:
            old_rank = original_ranking.index(compromised_node) + 1
            new_rank = sybil_ranking.index(compromised_node) + 1

            total_diff_percentage += float(old_rank) / float(new_rank)

        return float(total_diff_percentage) / float(len(compromised_nodes)), float(total_diff_values) / float(len(compromised_nodes))

    @experiment_callback
    def compute(self):
        results = []
        random_identities = random.sample(self.traders, 1000)
        sybil_region_size = int(os.environ['SYBIL_REGION_SIZE'])

        identity = random_identities[self.experiment.scenario_runner._peernumber - 1]
        pagerank_rank_diff, pagerank_rep_diff = self.compute_diff(sybil_region_size, identity, 'pagerank')
        personalised_rank_diff, personalised_rep_diff = self.compute_diff(sybil_region_size, identity, 'personalised_pagerank')
        temporal_rank_diff, temporal_rep_diff = self.compute_diff(sybil_region_size, identity, 'temporal_pagerank')

        results.append((identity, pagerank_rank_diff, pagerank_rep_diff, "PageRank"))
        results.append((identity, personalised_rank_diff, personalised_rep_diff, "Personalised PageRank"))
        results.append((identity, temporal_rank_diff, temporal_rep_diff, "Temporal PageRank"))

        with open("results.csv", "w") as results_file:
            for tup in results:
                results_file.write("%s,%s,%s,%s\n" % tup)

        reactor.stop()
