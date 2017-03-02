import re
job_Link = 'http://m.zhaopin.com/job/cc150681214j90250250000/'
job_Id = re.search(re.compile("(\d+)/$"), job_Link)
print(job_Id)
