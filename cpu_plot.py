import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np

def cal_htap(result_data, need_print=False):
    tpcc_cnt, tpcc_time, tpcc_tps, tpcc_tps_sum = [], [], [], 0
    tpch_cnt, tpch_time, tpch_cost, tpch_avg_cost, tpch_qpm, tpch_qpm_sum = [], [], [], [], [], 0
    for data_item in result_data:
        if data_item['type'] == 'tpcc':
            tmp_tpcc_cnt, tmp_tpcc_time, tmp_tpcc_tps = 0, 0, 0
            for k,v in data_item['success_count'].items():
                tmp_tpcc_cnt += v
            for k,v in data_item['exec_time'].items():
                tmp_tpcc_time += v
            tmp_tpcc_tps = tmp_tpcc_cnt/tmp_tpcc_time

            tpcc_cnt.append(tmp_tpcc_cnt), tpcc_time.append(tmp_tpcc_time), tpcc_tps.append(tmp_tpcc_tps)
        elif data_item['type'] == 'tpch':
            tmp_tpch_cnt, tmp_tpch_time, tmp_tpch_cost, tmp_tpch_avg_cost, tmp_tpch_qpm = [0]*22, [0]*22, [0]*22, 0, 0
            for i in range(22):
                tmp_tpch_cnt[i] += data_item['success_count'][i]
                tmp_tpch_time[i] += data_item['exec_time'][i]
            tmp_tpch_cost_cnt = 0
            for i in range(22):
                if tmp_tpch_cnt[i] == 0:
                    tmp_tpch_cost[i] = 0
                else:
                    tmp_tpch_cost_cnt+=1
                    tmp_tpch_cost[i] = tmp_tpch_time[i]/tmp_tpch_cnt[i]
            tmp_tpch_avg_cost = sum(tmp_tpch_cost) / tmp_tpch_cost_cnt
            tmp_tpch_qpm = 60/tmp_tpch_avg_cost
            
            tpch_cnt.append(tmp_tpch_cnt), tpch_time.append(tmp_tpch_time), tpch_cost.append(tmp_tpch_cost), tpch_avg_cost.append(tmp_tpch_avg_cost), tpch_qpm.append(tmp_tpch_qpm)
    tpcc_tps_sum = sum(tpcc_tps)
    tpch_qpm_sum = sum(tpch_qpm)
    if need_print:
        print(f'tpcc_tps: {tpcc_tps}\ntpcc_tps_sum: {tpcc_tps_sum}')
        print(f'tpch_avg_cost: {tpch_avg_cost}\ntpch_qpm: {tpch_qpm}\ntpch_qpm_sum: {tpch_qpm_sum}')
    return {
        'tpcc_cnt': tpcc_cnt,
        'tpcc_time': tpcc_time,
        'tpcc_tps': tpcc_tps,
        'tpch_cnt': tpch_cnt,
        'tpcc_tps_sum': tpcc_tps_sum,
        'tpch_time': tpch_time,
        'tpch_cost': tpch_cost,
        'tpch_avg_cost': tpch_avg_cost,
        'tpch_qpm': tpch_qpm,
        'tpch_qpm_sum': tpch_qpm_sum
    }


base_data_dir = '/Users/panfengguo/Downloads/metric_result'
test_case = {
    '2core': {
        'file_name' : f'{base_data_dir}/sf5_tp1_ap1/result_2024-12-04_12-57.json'
    },
    '4core': {
        'file_name' : f'{base_data_dir}/sf5_tp2_ap2/result_2024-12-04_14-10.json'
    },
    '8core': {
        'file_name' : f'{base_data_dir}/sf5_tp4_ap4/result_2024-12-04_14-56.json'
    },
    '16core': {
        'file_name' : f'{base_data_dir}/sf5_tp8_ap8/result_2024-12-04_15-19.json'
    },
    '32core': {
        'file_name' : f'{base_data_dir}/sf5_tp16_ap16/result_2024-12-04_16-06.json'
    },
    '64core': {
        'file_name' : f'{base_data_dir}/sf5_tp32_ap32_2/result_2024-11-05_23-04.json'
    },
}
result_data_map = {}
for case_name, case_item in test_case.items():
    with open(case_item['file_name'], 'r') as file:
        result_data_map[case_name] = json.load(file)

for case_name, case_data in result_data_map.items():
    print(f'case: {case_name}')
    test_case[case_name]['result'] = cal_htap(case_data, True)

# Data for the grouped bar chart
plt.rcParams['font.family'] = 'Times New Roman'
labels = ['2core', '4core', '8core', '16core', '32core', '64core']
tpcc_values = [test_case[label]['result']['tpcc_tps_sum'] for label in labels]
tpch_values = [test_case[label]['result']['tpch_qpm_sum'] for label in labels]

x = range(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax1 = plt.subplots(figsize=(12, 6))
rects1 = ax1.bar(x, tpcc_values, width, label='TP TPS', color='#fa7f6f')

# Create a second y-axis
ax2 = ax1.twinx()
rects2 = ax2.bar([p + width for p in x], tpch_values, width, label='AP QPM', color='#82b0d2')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax1.set_xlabel('Core Count')
ax1.set_ylabel('TP TPS')
ax2.set_ylabel('AP QPM')
ax1.set_title('TP TPS and AP QPM by Core Count')
ax1.set_xticks([p + width / 2 for p in x])
ax1.set_xticklabels(labels)

# Combine legends
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines + lines2, labels + labels2, loc='upper left')

ax1.grid(axis='y')
fig.savefig(f'./img/htap_core_count.svg', format='svg')
plt.show()