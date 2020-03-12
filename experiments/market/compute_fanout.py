import sys


def compute_probability(m, f, r):
    res = 1
    for i in range(f):
        res = res * ((m - int((1 - r) * f) - i) / m)

    return res


def determine_combination(network_size, error_prob, adversarial_rate):
    for fanout in range(1, 200):
        prob = compute_probability(network_size, fanout, adversarial_rate)
        if prob <= error_prob:
            return fanout

    exit(1)
    return -1


print(determine_combination(int(sys.argv[1]), 0.05, float(sys.argv[2])))