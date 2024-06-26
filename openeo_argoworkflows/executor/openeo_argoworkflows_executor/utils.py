from openeo_pg_parser_networkx import OpenEOProcessGraph

def get_pg_bounding_box(process_graph: dict):
    graph = OpenEOProcessGraph(pg_data=process_graph)

    load_calls = [
        value for key, value in graph.nodes  if "load_" in value["process_id"]
    ]
    for call in load_calls:
        if "spatial_extent" in call['resolved_kwargs']:
            return call["resolved_kwargs"]["spatial_extent"]


def derive_sub_graph(cell, process_graph: dict):

    print("Derive sub graph")

    west, south, east, north = cell[2].bounds

    for key, value in process_graph.items():
        print(f"Process graph iteration {key}")
        if "load_" in value["process_id"]:
            print("Update spatial extent")
            process_graph[key]["arguments"]["spatial_extent"] = {
                "west": west, "east": east, "south": south, "north": north
            }

    print("Return new graph ")
    return process_graph