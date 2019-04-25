import sys


def compute_probability(n, r):
    res = 1
    for i in range(r):
        res = res * ((n - r) / float(n))

    return res


def get_msg_reach(ttl, fanout):
    """
    Return the message reach, based on TTL/fanout
    """
    total = 0
    for ind in range(1, ttl+1):
        total += fanout ** ind
    return total


def determine_combination(network_size, error_prob):
    best_combination = None
    min_dist = 100
    for ttl in [1, 2, 3]:
        for fanout in range(1, 20):
            prob = compute_probability(network_size, get_msg_reach(ttl, fanout))
            if prob <= error_prob:
                diff = error_prob - prob
                if not best_combination or diff < min_dist:
                    best_combination = (fanout, ttl)
                    min_dist = diff

    return best_combination


fanout, ttl = determine_combination(int(sys.argv[1]), 0.05)

if sys.argv[2] == 'ttl':
    print ttl
elif sys.argv[2] == 'fanout':
    print fanout
