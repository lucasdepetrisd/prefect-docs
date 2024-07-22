from prefect import flow, task

@task
def simple_task():
    print("I am a simple task.")
    return "simple_task_result"

@flow
def dependent_flow(data):
    print(f"Received data: {data}")
    simple_task()

@flow
def upstream_flow():
    result = simple_task()
    dependent_flow(result)

if __name__ == "__main__":
    upstream_flow()