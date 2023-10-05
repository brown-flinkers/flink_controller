# Vertex Class Definition
class Vertex:
    def __init__(self, name, processing_rate, parallelism=1):
        self.name = name
        self.processing_rate = processing_rate
        self.parallelism = parallelism
        self.adjacent = set()

    def add_neighbor(self, vertex):
        if isinstance(vertex, Vertex):
            self.adjacent.add(vertex)

    def get_neighbors(self):
        return self.adjacent

# DirectedGraph Class Definition
class DirectedGraph:
    def __init__(self, input_rate):
        self.vertices = {}
        self.input_rate = input_rate
        self.source = None
        self.sink = None

    def add_vertex(self, vertex):
        if isinstance(vertex, Vertex):
            self.vertices[vertex.name] = vertex

    def get_vertex(self, name):
        return self.vertices.get(name)

    def set_source(self, vertex):
        if isinstance(vertex, Vertex):
            self.source = vertex

    def set_sink(self, vertex):
        if isinstance(vertex, Vertex):
            self.sink = vertex

    def add_edge(self, from_vertex, to_vertex):
        if from_vertex.name in self.vertices and to_vertex.name in self.vertices:
            from_vertex.add_neighbor(to_vertex)

    def topological_sort(self):
        stack = []
        visited = set()

        def dfs(vertex):
            visited.add(vertex.name)
            for neighbor in vertex.get_neighbors():
                if neighbor.name not in visited:
                    dfs(neighbor)
            stack.append(vertex)

        dfs(self.source)
        return stack[::-1]

    def compute_output_rates(self):
        if not self.source:
            raise ValueError("Source node is not set. Cannot compute output rates.")
        output_rates = {}
        output_rates[self.source.name] = min(self.input_rate, self.source.parallelism * self.source.processing_rate)

        for vertex in self.topological_sort():
            if vertex != self.source:
                aggregated_output = sum([output_rates[v.name] for v in self.vertices.values() if vertex in v.get_neighbors()])
                output_rates[vertex.name] = min(vertex.parallelism * vertex.processing_rate, aggregated_output)

        return output_rates

    def get_sink_input_rate(self, output_rates):
        if not self.sink:
            raise ValueError("Sink node is not set. Cannot compute its input rate.")
        return output_rates.get(self.sink.name, None)

# Example Graphs and Calculations
graph1 = DirectedGraph(1000)
graph2 = DirectedGraph(2000)
graph3 = DirectedGraph(3000)

# Example 1
# Source -> A -> B -> Sink
A1 = Vertex("A", 500, 2)
B1 = Vertex("B", 300, 3)
graph1.add_vertex(A1)
graph1.add_vertex(B1)
graph1.set_source(A1)
graph1.set_sink(B1)
graph1.add_edge(A1, B1)

# Example 2
#      A
#    /   \
# Source   B -> Sink
#    \   /
#      C
A2 = Vertex("A", 700, 2)
B2 = Vertex("B", 400, 5)
C2 = Vertex("C", 350, 4)
graph2.add_vertex(A2)
graph2.add_vertex(B2)
graph2.add_vertex(C2)
graph2.set_source(A2)
graph2.set_sink(B2)
graph2.add_edge(A2, B2)
graph2.add_edge(A2, C2)
graph2.add_edge(C2, B2)

