Phoenix Project
MapReduce Scheduler README
Last revised 15th April 2007


1. Phoenix Scheduler Overview
-----------------------------

The Phoenix Project had the goal of creating a Multi-Processor/Multi-Core
version of Google's MapReduce platform. The Phoenix scheduler provides a 
Map-Reduce engine that applications running on SMP or CMP machines can use to 
parallelize their tasks effectively and simply. 

To read about the Phoenix Project in detail:
   http://csl.stanford.edu/~christos/publications/2007.cmp_mapreduce.hpca.pdf

To learn more about Google's MapReduce Platform upon which the Phoenix project
is based:
   http://labs.google.com/papers/mapreduce.html


2. Provided Files
-----------------

MapReduceScheduler.c/h: The MapReduce scheduler engine that acts as the control
                        interface for coordinating the various threads and 
                        managing the data structures
Makefile: The Makefile here is provided as a template. Since the MapReduce 
          scheduler is meant to be compiled into applications and does not 
          execute as a standalone, running make in the directory would not make
          sense. We provide the Makefile so that users can use it as a template
          to compile their own applications.
README.txt: This file.


3. Target Operating Environments
--------------------------------

The Phoenix MapReduce Engine currently only runs on Solaris OS. It has been
tested on the Sun Fire T1200 (UltraSparc T1 processor, CMP) and on the Sun 
Ultra-Enterprise 6000 (UltraSparc II processor, SMP). There is no support for 
Linux environments at this time, although it would be simple to port the code to
Linux (see Section on Porting below). 


4. Using Phoenix with your applications
---------------------------------------

To use the Phoenix MapReduce Engine with your application, the application must
make a call to 'map_reduce_scheduler(scheduler_args_t *)'. This is the 
entry-point into the MapReduce engine. The application should also provide, as
part of the sheduler_args_t data-structure, a pointer to the map and reduce
functions, as well as a pointer to the key-comparison function. The best way
to get started is to study the example applications provided as part of this
distribution. For more details on other optional parameters and what they mean,
please refer to our paper at
   http://csl.stanford.edu/~christos/publications/2007.cmp_mapreduce.hpca.pdf


5. Porting Phoenix to other platforms
-------------------------------------

Currently, The MapReduce engine provided by Phoenix works only on Solaris 
machines. This limitation arises because we use the following solaris-specific
functions to query the status of the processors available in the machine and 
bind threads to specific processors:

   sysconf()
   p_online()
   processor_bind()

There are functions that one could use in a Linux environment to perform the 
same tasks. These are:

   sched_getaffinity()
   isCpuAvailable()
   sched_setaffinity()
   
The Makefile provided with the MapReduce Engine defines constants based on the 
OS where the MapReduce Engine is being compiled that can be used within the 
source file. As an example, we have included differentiated code for Solaris
vs. Linux, by using the following construct

#ifdef _SOLARIS_ 
   .   .   .
#endif
#ifdef _LINUX_ 
   .   .   .
#endif

Other operating systems can be supported in a similar fashion. The Makefile
already defines such constants for Solaris, Linux, Cygwin and Darwin. 

Similarly, although the MapReduce scheduler uses the Pthreads libraries to
achieve parallelism, the code can be ported to other threading environments by 
replacing all calls to the Pthreads library to equivalent calls in other 
threading libraries. 

Important Note: The Linux specific code provided within the 
#ifdef _LINUX_ 
   ... 
#endif
constructs are meant as a reference template for how one would port this
library to Linux, and has not been tested in Linux. Therefore, This library 
CANNOT BE GUARANTEED TO COMPILE OR FUNCTION CORRECTLY IN ANY ENVIRONMENT OTHER 
THAN SOLARIS.


6. Bugs / Suggestions
---------------------

Please contact christos@ee.stanford.edu to report any bugs or to suggest
improvements.

