def reducer(key, value_iterator):
    mylist = list(value_iterator)
    yield key, [sum(mylist), len(mylist)]
