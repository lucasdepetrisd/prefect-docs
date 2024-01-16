from prefect import task, Flow, Parameter

@task
def add_numbers(x, y):
    result = x + y
    return result

@task
def multiply_by_two(x):
    result = x * 2
    return result

with Flow("testing_flow") as flow:
    x_param = Parameter("x", default=5)
    y_param = Parameter("y", default=10)

    added = add_numbers(x_param, y_param)
    multiplied = multiply_by_two(added)

if __name__ == "__main__":
    flow.run()
