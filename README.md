# Adapted IndexAdvisor
Originally released as part of the paper An Index Advisor Using Deep Reinforcement Learning by Lan, Bao, and Peng

We modified some parts of it to enable other workloads, TPC-DS and Join Order Benchmark, and to evaluate it with workloads from our paper.
The below original README is slightly updated.

# Index Advisor based on Deep Reinforcement Learning
Code for original CIKM2020 [paper](https://dl.acm.org/doi/abs/10.1145/3340531.3412106)

# What does it do?
This is an index advisor tool to recommend an index configuration for a certain workload under maximum storage or index number. It combines the heuristic rules and deep reinforcement learning together.

# What do I need to run it?
1. You should install a [PostgreSQL](https://www.postgresql.org/) database instance with [HypoPG extension](https://hypopg.readthedocs.io/en/latest/).
2. You should install the required python packages (see requirements.txt via pip).
3. The code works for TPC-H (SF 10), TPC-DS (SF 10), and Join Order Benchmark databases. However, you need to provide the databases yourself. The [index selection evaluation framework](https://github.com/hyrise/index_selection_evaluation) provides an easy way
to set up the database systems.

# How do I run it?
1. You can find the entry in ./EntryM3DP.py
2. Before, candidates must be generated with ./Sample4GenCandidates.py. This file should be self-explanatory. Query files are included in the JOB, TPC-DS, and TPC-H folders.
