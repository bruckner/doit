import math

def numericize(vals):
    nums = []
    for i in range(0, len(vals)):
        try:
            nums.append(float(vals[i]))
        except ValueError:
            pass
    return nums

def avg(nums):
    return float(sum(nums)) / len(nums)

def stddev(nums):
    sqrs = [x**2 for x in nums]
    return math.sqrt(float(sum(sqrs) - float(sum(nums)**2) / len(nums)) / len(nums))

def bucketize(vals):
    nums = numericize(vals)

    if len(nums) <= 1:
        return dict()

    n_buckets = math.ceil(math.sqrt(len(nums)))

    dv = stddev(nums)
    av = avg(nums)
    mx = min(av + 3 * dv, max(nums))
    mn = max(av - 3 * dv, min(nums))
    nums = [n_buckets * (x - mn) / (mx - mn) for x in nums]

    buckets = [0 for i in range(0, n_buckets)]
    overflow = 0
    underflow = 0

    for x in nums:
        try:
            buckets[int(x)] = buckets[int(x)] + 1
        except IndexError:
            if x < 0:
                underflow = underflow + 1
            else:
                overflow = overflow + 1

    return {'min': mn, 'max': mx, 'avg': av, 'stddev': dv, 'n_buckets': n_buckets, 'buckets': buckets, 'overflow': overflow, 'underflow': underflow}




