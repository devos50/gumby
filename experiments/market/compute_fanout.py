import sys


def compute_probability(n, r):
    res = 1
    for i in range(r):
        res = res * ((n - r - i) / float(n))

    return res


def determine_combination(network_size, error_prob):
    for fanout in range(1, 200):
        prob = compute_probability(network_size, fanout)
        if prob <= error_prob:
            return fanout

    exit(1)
    return -1


print(determine_combination(int(sys.argv[1]), 0.1))