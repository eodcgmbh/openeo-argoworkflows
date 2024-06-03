from hera.workflows import Steps, Workflow, script, WorkflowsService

@script()
def print_graph(graph: dict):
    from time import sleep
    sleep(30)
    print(graph)

def hello_world(service: WorkflowsService, process_graph: dict):
    with Workflow(
        generate_name="openeo-workflow-",
        entrypoint="steps",
        namespace="default",
        workflows_service=service
    ) as w:
        with Steps(name="steps"):
            print_graph(arguments={"graph": process_graph})
    return w
