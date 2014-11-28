Phoenix Project
Top README File
Last revised 15th April 2007

1. What is Phoenix
------------------

Phoenix is shared-memory implementation of Google's MapReduce model
for data-intensive processing tasks. Phoenix can be used to program
multi-core schips as well as shared-memory multiprocessors (SMPs and
ccNUMAs).  Phoenix was developed as a class project for the EE382a
class at Stanford (Advanced Processor Architecture). The paper on
Phoenix won the best paper award in the HPCA'07 conference. 

The current distribution of Phoenix runs only on the Solaris operating
system. There is no support for Linux environments at this time,
although it would be simple to port the code to any OS with support
for shared-memory threads (see the Porting notes in the scheduler
README file).

The Phoenix webpage is: http://csl.stanford.edu/~christos/sw/phoenix


2. What is Provided
-------------------

The Phoenix distribution includes the following directories:

docs:   The HPCA'07 paper and slides on Phoenix as well as the
        original OSDI'04 MapReduce paper by Google. 

phoenix_scheduler: the source code for the Phoenix scheduler for
        MapReduce programs. The scheduler includes all the performance
        features discussed in the HPCA'07 paper. This version does not
        support any error injection, detection, or recovery features.

sample_apps:  the source code for the applications used to evaluate
        Phoenix for the HPCA'07 paper. For each application, we
        provide MapReduce, sequential, and p-threads code for
        comparison purposes. 
 
Note: There are two tar files available for the Phoenix
  distribution. The tiny package (phoenix_tiny.tar.gz) 
  includes the API and scheduler code as well as three sample
  applications that use randomly generated input data. 
  The regular package (phoenix_regular.tar.gz) includes 
  five additional application along with small input datasets 
  for testing purposes. We distribute a full set of input datasets
  (typically small, medium, and large) for these apps in 
  separate files. Visit the Phoenix webpage for more information.

3. License & Credit
--------------------

Phoenix source code is distributed with a BSD-style license. The
copyright is held by Stanford University. Phoenix is provided "as is"
without any guarantees of any kind.

If you use Phoenix in your work or research, consider referencing the
HPCA'07 paper: 

 "Evaluating MapReduce for Multi-core and Multiprocessor Systems",
  Colby Ranger, Ramanan Raghuraman, Arun Penmetsa, Gary Bradski,
  Christos Kozyrakis.  Proceedings of the 13th Intl. Symposium on
  High-Performance Computer Architecture (HPCA), Phoenix, AZ, February
  2007"


4. Contact Information
-----------------------

Please contact Christos Kozyrakis (christos@ee.stanford.edu) for
further information. We would appreciate if you let us know that you
are using our system. While we may not be able to provide technical
support, please let us know about bug reports and suggested
improvements. If you make some correction or improvement to Phoenix,
we would appreciate receiving a copy that we can include in the next
release.

The Phoenix webpage is: http://csl.stanford.edu/~christos/sw/phoenix


