def pytest_itemcollected(item):
    def collect_nodes(item, nodes=[]):
        if not (hasattr(item, "parent") and hasattr(item.parent, "obj")):
            return nodes
        nodes.append(item.obj)
        return collect_nodes(item.parent, nodes)

    nodes = collect_nodes(item)[::-1]
    node_ids = (x.__doc__.strip() if x.__doc__ else x.__name__ for x in nodes)
    item._nodeid = ":".join(node_ids)
