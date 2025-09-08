def reducer(key, value_iterator):
    mystery = [x for x in value_iterator if x > 1]
    yield key, sum(mystery)
