# Instructions

In order to run the application in my environment I used the following environment  
variables:  

export SPARK_HOME = \<path to your spark distro\>  
export SPARK_LOCAL_IP="127.0.0.1  

I also executed the python code from within a virtual env with the dependencies defined  
in the `requirements.txt` file.

To run the app:  

`python3 metrics.py`  

To run the unit tests:

`python3 -m unittest metrics_test.py`
