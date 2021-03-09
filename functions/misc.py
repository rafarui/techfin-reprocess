def find_keys(node, kv):
    if isinstance(node, list):
        for i in node:
            for x in find_keys(i, kv):
                yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in find_keys(j, kv):
                yield x
                
def unroll_list(l):
    if isinstance(l, list):
        for i in l:
            for x in unroll_list(i):
                yield x
    else:
        yield l