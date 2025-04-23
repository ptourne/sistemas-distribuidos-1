import os
# This is a script that tests the integrity if the system. It will call ./generate-compose.sh with filter count, and reducer count, will build the docker services and run it.
# Then, it will wait untill all services had stopped, except for rabbitmq. It will ensure that all containers exited with code 0. If not, it will save the logs to a file.
# Finnaly, it will grab the time it took to run the test and save it to a file, together with the number of filters and reducers used.
# It will repeat the test 10 times with each tuple of counts
from time import sleep
import subprocess

MIN_FILTERS = 1
MAX_FILTERS = 20
MIN_REDUCERS = 1
MAX_REDUCERS = 20
ITERATIONS = 10
FAILED_TEST_LOGS = "./test_results/failed"
PASSED_TEST_RESULTS = "./test_results/success.csv"


def docker_down():
    subprocess.check_output(
        ["docker", "compose", "down", "--volumes", "--remove-orphans"])


def generate_compose_file(filters, reducers):
    # call command in shell
    subprocess.check_output(
        ["./generate-compose.sh", str(filters), str(reducers), str(reducers)])


def docker_build():
    subprocess.check_output(["docker", "compose", "build"])


def docker_up():
    subprocess.check_output(["docker", "compose", "up", "-d"])


def get_logs(container_name):
    return subprocess.check_output(["docker", "logs", container_name]).decode()


def docker_wait():
    subprocess.check_output(["docker", "compose", "wait", "coordinator"])


class LogLine:
    def __init__(self, timestamp, service, level, message):
        self.timestamp = timestamp
        self.service = service
        self.level = level
        self.message = message


def log_message(line):
    # Line format: 11:38:09.467348  coordinator  INFO   Film matched expected

    # Split the line into parts
    message = line[len("11:38:09.467348  coordinator  INFO   "):]
    return message


def all_succeded():
    # get exit code of containers
    res = subprocess.check_output(
        ["docker", "ps", "-a", "--format", "{{.Names}} {{.Status}}"]).decode()
    # Example exit
    # 68e261b81529 Exited (0) 7 minutes ago
    # 025cc78519f8 Exited (0) 7 minutes ago
    # a8dcba31877f Exited (0) 7 minutes ago
    # b079c140c345 Exited (0) 7 minutes ago
    # c40286ee2e4b Up 8 minutes (healthy)

    # Check if all containers exited with code 0
    print("checking if all containers exited with code 0")
    for line in res.splitlines():
        print(f"docker ps line: {line}")
        container_name = line.split(' ')[0]
        if container_name == "rabbitmq":
            continue
        if "Exited (0)" not in line:
            print(f"Container {container_name} did not exit with code 0")
            return False, None

    lines = get_logs("coordinator").splitlines()
    passed = False
    print("reading coordinator logs")
    for line in lines:
        print("coordinator log line: ", line)
        msg = log_message(line)
        if "TESTING ALL | End | Passed" in msg:
            passed = True
            break
    if not passed:
        print("Tests failed")
        return False, None
    execution_time = None
    for line in lines:
        msg = log_message(line)
        if "Execution time: " in msg:
            execution_time = msg.split(": ")[1]
    return True, execution_time


def path_to_save_logs(filter, reducer, iteration, service):
    parent = f"{FAILED_TEST_LOGS}/{filter}_{reducer}/{iteration}"
    os.makedirs(f"{parent}", exist_ok=True)
    return f"{parent}/{service}"


def docker_save_logs(filters, reducers, iteration):
    res = subprocess.check_output(
        ["docker", "ps", "-a", "--format", "{{.Names}} {{.Status}}"]).decode()
    services = res.splitlines()

    for service in services:
        name = service.split(' ')[0]
        if name == "rabbitmq":
            continue
        path = path_to_save_logs(filters, reducers, iteration, name)
        with open(path, "w") as f:
            output = get_logs(name)
            f.write(output)


def save_results(filters, reducers, iteration, passed, execution_time):
    with open(PASSED_TEST_RESULTS, "a") as f:
        f.write(f"{filters},{reducers},{iteration},{passed},{execution_time}\n")


def process_results(filters, reducers, iteration):
    passed, execution_time = all_succeded()
    if not passed:
        print("Not all containers exited with code 0")
        docker_save_logs(filters, reducers, iteration)
    else:
        print(f"All containers exited with code 0")
    save_results(filters, reducers, iteration, passed, execution_time)


def main():
    docker_down()
    for filters in range(MIN_FILTERS, MAX_FILTERS + 1):
        for reducers in range(MIN_REDUCERS, MAX_REDUCERS + 1):
            print(f"Starting tests for filters {filters} reducers {reducers}")
            generate_compose_file(filters, reducers)
            docker_build()
            for iteration in range(ITERATIONS):
                print(f"Iteration {iteration}")
                print("docker up")
                docker_up()
                print("waiting")
                docker_wait()
                print("processing results")
                process_results(filters, reducers, iteration)
                print("docker down")
                docker_down()
                sleep(1)


if __name__ == "__main__":
    main()
