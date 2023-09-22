# Machine Learning Pipeline Optimizer
This Python script optimizes a machine-learning data pipeline. It takes a pipeline file and the number of CPU cores as inputs. The pipeline file should define the tasks in the pipeline, each with an execution time, a group, and dependencies.

The script computes the minimum execution time of the pipeline, considering that at most cpu_cores number of tasks can be executed in parallel. All tasks in a group should be executed before proceeding with another group, except for tasks that do not belong to any group.

The script also outputs a scheduling diagram of the optimal solution.

The script assumes we are dealing with a directed acyclic graph (DAG) and utilizes topological sorting and job scheduling greedy algorithms to order the execution of the tasks at hand.
## Usage
The script accepts two command line arguments: --pipeline and --cpu_cores.

Example:

`python main.py --cpu_cores=2 --pipeline=pipeline.txt`
## Pipeline File Structure
Each pipeline file consists of 4*n + 1 lines, describing the tasks in the pipeline: 4 lines for each task and a terminating line with the value END. The properties of each task are described in 4 consecutive lines:

Task name
Execution time (in whole minutes)
Group name (empty line if no group)
Comma-separated list of task names that this task depends on (empty line if no group)
Example:
```txt
A
2
feature

B
1
feature

C
2
model
B
END
```
## Output
The script outputs the minimum execution time of the pipeline and a scheduling diagram of the optimal solution.

Example:
```sh
| Time    | Tasks being Executed | Group Name
| ------- | -------------------- | ----------
|  1      | A,B                  | feature                         
|  2      | A                    | feature
|  3      | C,C                  | model

Minimum Execution Time = 3 minutes.
```
## Logging
The script logs errors to a file named error.log. The log file includes the timestamp, log level, and error message.

## Requirements
The script requires Python 3.6 or later. It uses the following Python standard libraries: collections, typing, argparse, and logging.