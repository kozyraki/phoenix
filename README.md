phoenix
=======

___What is Phoenix?___

Phoenix is a shared-memory implementation of Google's MapReduce model for data-intensive processing tasks. Phoenix can be used to program multi-core chips as well as shared-memory multiprocessors (SMPs and ccNUMAs).  Phoenix was developed as a class project for the EE382a course at Stanford. The paper on Phoenix won the best paper award in the HPCA'07 conference.

The initial version of Phoenix was released in the April of 2007. Since then it has been significantly updated to improve scalability and portability. Compared to the original release, version 2 includes:

Linux (x86_64) support
Enhanced NUMA support for Solaris environment
Improved performance and stability
About Phoenix++

Phoenix++ is a C++ reimplementation of Phoenix 2. It is designed to provide a modular, extensible pipeline that can be easily adapted by the user to the characteristics of a particular workload.

___Distributions___

You have the following distribution options:
- Phoenix++
- Phoenix 2
- Phoenix 1
- Sample apps (8)

Full input datasets for 5 of the sample applications. We provide small, medium, and large datasets for each application. The other 3 sample applications operate on randomly generated data. 
- [histogram](http://csl.stanford.edu/~christos/data/histogram.tar.gz)(~512 MB)
- [linear regression](http://csl.stanford.edu/~christos/data/linear_regression.tar.gz) (~212 MB)
- [string match](http://csl.stanford.edu/~christos/data/string_match.tar.gz) (~212 MB)
- [reverse index](http://csl.stanford.edu/~christos/data/reverse_index.tar.gz) (~154 MB)
- [word count](http://csl.stanford.edu/~christos/data/word_count.tar.gz) (~59 MB)

___License___

The Phoenix source code is distributed with a BSD license. The copyright is held by Stanford University. Phoenix is provided "as is" without any guarantees of any kind.

If you use Phoenix in your work or research, please let us know about it. We also encourage you to reference one of these papers:

"Phoenix++: Modular MapReduce for Shared-Memory Systems", 
Justin Talbot, Richard M. Yoo, and Christos Kozyrakis.
In the Second International Workshop on MapReduce and its Applications (MAPREDUCE),
San Jose, CA, June 2011. [paper][slides]

"Phoenix Rebirth: Scalable MapReduce on a Large-Scale Shared-Memory System", 
Richard M. Yoo, Anthony Romano, and Christos Kozyrakis.
In Proceedings of the 2009 IEEE International Symposium on Workload Characterization (IISWC),
pp. 198-207, Austin, TX, October 2009. [paper][slides]

"Evaluating MapReduce for Multi-core and Multiprocessor Systems", 
Colby Ranger, Ramanan Raghuraman, Arun Penmetsa, Gary Bradski, and Christos Kozyrakis.
In Proceedings of the 13th Intl. Symposium on High-Performance Computer Architecture (HPCA),
Phoenix, AZ, February 2007. [paper][slides][video] (Best Paper Award)
Mailing List

We have created a mailing list for Phoenix users (phoenix-users@lists.stanford.edu). The goal is to provide a forum to communicate issues / fixes and exchange ideas about Phoenix and MapReduce programming in general. To receive the messages, please register. The messages are also archived. Note that the existence of the mailing list does not come along with support guarantees... 

___Related Projects___

Below are some of the external projects / publications that use Phoenix.
Tongping Liu, Charlie Curtsinger, and Emery D. Berger. "Dthreads: Efficient Deterministic Multithreading." In Proceedings of the 23rd ACM Symposium on Operating Systems Principles, 2011.

Tongping Liu and Emery D. Berger. "Precise Detection and Automatic Mitigation of False Sharing." In Proceedings of the 26th Annual ACM SIGPLAN Conference on Object-Oriented Programming, Systems, Languages, and Applications, 2011

Wei Jiang et al. "A Map-Reduce System with an Alternate API for Multi-Core Environments." In Proceedings of the 10th IEEE/ACM International Symposium on Cluster, Cloud, and Grid Computing, 2010.

Tongping Liu and Emery Berger. "Sheriff: Detecting and Eliminating False Sharing." Technical Report UMass TR UM_CS-2010-047, University of Massachusetts, Amherst.

R. Chen et al. "Tiled-MapReduce: Optimizing Resource Usages of Data-parallel Applications on Multicore with Tiling." In Proceedings of the 19th International Conference on Parallel Architectures and Compilation Techniques, 2010.

Adam Gordon. Elastic Phoenix. University of Alberta.

Yandong Mao et al. "Optimizing MapReduce for Multicore Architectures." Technical Report MIT-CSAIL-TR-2010-020, MIT.

Silas Boyd-Wickizer et al. "Corey: An Operating System for Many Cores." In Proceedings of the 8th Symposium on Operating Systems Design and Implementation, 2008

___Credits___

Phoenix++ was developed by Justin Talbot and Richard Yoo.
Phoenix 2 was developed by Richard Yoo and Anthony Romano.
Original Phoenix code was developed by Colby Ranger, Ramanan Raghuraman, and Arun Penmetsa under the supervision of Christos Kozyrakis.
Gary Bradski provided valuable feedback and the original version for some of the sample applications.
Emery Berger provided a Linux port for the original Phoenix.

___Contact Information___

Please contact Christos Kozyrakis (kozyraki@stanford.edu) for further information. While we may not be able to provide technical support, let us know about bug reports and suggested improvements. If you make some correction or improvement to Phoenix, we would appreciate receiving a copy that we can include in the next release.
