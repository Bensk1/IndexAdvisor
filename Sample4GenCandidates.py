import pickle
import psqlparse
from Preprocess import Dataset as ds
from Utility import Encoding as en
from Utility import ParserForIndex as pi


enc = en.encoding_schema()
# path to your tpch_directory/dbgen
work_dir = "/Users/lanhai/XProgram/PycharmProjects/2.18.0_rc2/dbgen"
w_size = 14
wd_generator = ds.TPCH(work_dir, w_size)
# workload = wd_generator.gen_workloads()

parser = pi.Parser(enc['attr'])


# def gen_i(__x):
#     added_i = set()
#     for i in range(len(workload)):
#         if i > __x:
#             continue
#         b = psqlparse.parse_dict(workload[i])
#         parser.parse_stmt(b[0])
#         parser.gain_candidates()
#         if i == 8:
#             added_i.add('lineitem#l_shipmode')
#             added_i.add('lineitem#l_orderkey,l_shipmode')
#             added_i.add('lineitem#l_shipmode,l_orderkey')
#     f_i = parser.index_candidates | added_i
#     f_i = list(f_i)
#     f_i.sort()
#     with open('cands'+str(__x+1)+'.pickle', 'wb') as df:
#         # pickle.dump(list(f_i), df, protocol=0)
#         print(f_i)

def candidates_(q):
    b = psqlparse.parse_dict(q)
    # try:
    parser.parse_stmt(b[0])
    # except:
        # print(q)
        # assert 1==2
    parser.gain_candidates()
    f_i = parser.index_candidates
    # with open('cands'+str(__x+1)+'.pickle', 'wb') as df:
        # pickle.dump(list(f_i), df, protocol=0)
    return f_i


# for i in range(0, 14):
#     gen_i(i)

workload = []

#JOB
for i in range(1, 114):
    with open(f"JOB/JOB_{i}.txt") as f:
        content = f.readlines()
        workload.append(content[0])
assert(len(workload) == 113)

# d = "["
# for w in workload:
#     a = w.replace('\n', '')
#     a = a.replace('"', '\\"')
#     d+= f'["{a}"], '
# d+="]"
# print(d)
# assert 1==2

#DS
# Because of parsing errors
# EXCLUDED_DS = [1,11,14,2,23,24,30,31,4,74]
# Because of evaluation reasonsp
# EXCLUDED_DS = [4, 6, 9, 10, 11, 32, 35, 41, 95]
# for i in range(1, 100):
#     if i in EXCLUDED_DS:
#         continue
#     with open(f"TPCDS/TPCDS_{i}.txt") as f:
#         content = f.readlines()
#         workload.append(content[0])
# assert(len(workload) == 99 - len(EXCLUDED_DS))

#H
# Because of evaluation reasons. 15 because parser is not capable
# EXCLUDED_H = [2,17,20]
# for i in range(1, 23):
#     if i in EXCLUDED_H:
#         continue
#     with open(f"TPCH/TPCH_{i}.txt") as f:
#         content = f.readlines()
#         workload.append(content[0])
# assert(len(workload) == 22 - len(EXCLUDED_H))

global_candidates = set()
for query in workload:
    querys_candidates = candidates_(query)
    for querys_candidate in querys_candidates:
        global_candidates.add(querys_candidate)

print(global_candidates)
print(len(global_candidates))

with open('cands_paper_job.pickle', 'wb') as df:
    pickle.dump(list(global_candidates), df, protocol=0)


