def mapper(key, line):
    parts = line.split("\t")
    yield [parts[2], parts[1]], int(parts[0]) * int(parts[3])
