Number = int | float
Comparable = Number | str


class ResourceList:
    def __init__(self, resource_list: list[Comparable]):
        self.resource_list = resource_list

    def __le__(self, other):
        return all([a <= b for a, b in zip(self.resource_list, other.resource_list)])

    def __lt__(self, other):
        return all([a < b for a, b in zip(self.resource_list, other.resource_list)])

    def __ge__(self, other):
        return all([a >= b for a, b in zip(self.resource_list, other.resource_list)])

    def __gt__(self, other):
        return all([a > b for a, b in zip(self.resource_list, other.resource_list)])

    def __add__(self, other):
        return ResourceList([a + b for a, b in zip(self.resource_list, other.resource_list)])

    def __sub__(self, other):
        res = []
        for a, b in zip(self.resource_list, other.resource_list):
            if a - b < 0:
                raise ValueError("Subtraction results in negative value")
            res.append(a - b)
        return ResourceList(res)

    def __iadd__(self, other):
        self.resource_list = [a + b for a, b in zip(self.resource_list, other.resource_list)]
        return self

    def __isub__(self, other):
        res = []
        for a, b in zip(self.resource_list, other.resource_list):
            if a - b < 0:
                raise ValueError("Subtraction results in negative value")
            res.append(a - b)
        self.resource_list = res
        return self
