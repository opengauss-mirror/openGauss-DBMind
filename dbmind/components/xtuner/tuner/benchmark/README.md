# Implementation
If you want to implement your benchmark which simulate your 
production scenario, you should do as the following.
**Firstly, we assume that xxx is your benchmark name.**

* Set file name as ```xxx.py``` in the benchmark directory;
* Implement a function named 'run' in xxx.py and declare two global variables of the string type, one called 'path' and the other called 'cmd'.;
* The signature of `run()` must be ```def run(remote_server_ssh, local_host_ssh) -> float```, that 
means you could use two SSH sessions to implement your code logic if they are useful, and you should 
return a numeric value as feedback;
* You can set xxx, the name of benchmark you want to run, in the xtuner.conf. The parameter name is `benchmark_script`.
