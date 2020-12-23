"""
住院异常项目阈值
"""

threshold = {'public': {}, 'community': {}, 'private': {}, 'private_other': {}}

threshold['public']['med_p_low'] = 0.01  # 药占比低阈值
threshold['public']['med_p_high'] = 0.75  # 药占比高阈值
threshold['public']['exam_p'] = 0.75  # 检查项目占比阈值
threshold['public']['th_time'] = 30  # 住院时间阈值（单位：天）
threshold['public']['th_avg_fund'] = 20000  # 均次费用阈值（单位：元）
threshold['public']['th_org'] = 1  # 住院机构数阈值
threshold['public']['th_cnt'] = 4  # 住院次数阈值
threshold['public']['th_age'] = 60  # 年龄阈值

threshold['community']['med_p_low'] = 0.01  # 药占比低阈值
threshold['community']['med_p_high'] = 0.75  # 药占比高阈值
threshold['community']['exam_p'] = 0.75  # 检查项目占比阈值
threshold['community']['th_time'] = 30  # 住院时间阈值（单位：天）
threshold['community']['th_avg_fund'] = 20000  # 均次费用阈值（单位：元）
threshold['community']['th_org'] = 1  # 住院机构数阈值
threshold['community']['th_cnt'] = 4  # 住院次数阈值
threshold['community']['th_age'] = 60  # 年龄阈值

threshold['private']['med_p_low'] = 0.01  # 药占比低阈值
threshold['private']['med_p_high'] = 0.75  # 药占比高阈值
threshold['private']['exam_p'] = 0.75  # 检查项目占比阈值
threshold['private']['th_time'] = 30  # 住院时间阈值（单位：天）
threshold['private']['th_avg_fund'] = 20000  # 均次费用阈值（单位：元）
threshold['private']['th_org'] = 1  # 住院机构数阈值
threshold['private']['th_cnt'] = 4  # 住院次数阈值
threshold['private']['th_age'] = 60  # 年龄阈值

threshold['private_other']['med_p_low'] = 0.01  # 药占比低阈值
threshold['private_other']['med_p_high'] = 0.75  # 药占比高阈值
threshold['private_other']['exam_p'] = 0.75  # 检查项目占比阈值
threshold['private_other']['th_time'] = 30  # 住院时间阈值（单位：天）
threshold['private_other']['th_avg_fund'] = 20000  # 均次费用阈值（单位：元）
threshold['private_other']['th_org'] = 1  # 住院机构数阈值
threshold['private_other']['th_cnt'] = 4  # 住院次数阈值
threshold['private_other']['th_age'] = 60  # 年龄阈值
