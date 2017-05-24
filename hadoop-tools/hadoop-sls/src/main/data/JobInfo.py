import datetime
import json
import sys

normalize_time = datetime.datetime(2016, 11, 1, 0, 0, 0, 0)

#date = sys.argv[1]
try:
	with open("temp.json", 'r') as content:
		job_status_repo = json.loads(content.read())
except:
	job_status_repo = {}

out = open("gcr_finished_job.txt", 'w')
# finished jobs
#print job_status_repo
cnt = 0
total_jobs = 0
kill_in_queue = 0
end_greater_start = 0
all_retries_within_five_seconds = 0
total_retries = 0
kill_in_queue_retries = 0
fail_in_queue_retries = 0
pass_in_queue_retries = 0
missing_retries = 0
weird_retries = 0
tmp = 0
weird_app = ""
json_out = open("gcr_sls_jobs.json", "w")
job_dont_have_end_time = 0
for job_status in job_status_repo["finishedJobs"]:
	# commit newly finished jobs
	appID = job_status["appID"]
	mapping = job_status["details"]
	arriveTime = job_status["queueDateTime"]
	startTime = job_status["startDateTimes"]
	endTime = job_status["finishDateTimes"]
	retries = job_status["retries"]
	user = job_status["username"]
	vc = job_status["vc"]
	queue = job_status["queue"]
	gpu = job_status["name"].split('!')[-1];
	status = job_status["status"]
	flag = True
	submitTime = datetime.datetime.strptime(arriveTime, '%Y-%m-%d %H:%M:%S')
	#start_time = datetime.datetime.strptime(startTime[0], '%Y-%m-%d %H:%M:%S')
	if (endTime == []):
	    flag = False
	    job_dont_have_end_time += 1
	else:
	    end_time = datetime.datetime.strptime(endTime[-1], '%Y-%m-%d %H:%M:%S')
	if (submitTime < normalize_time):
		continue;
	out.write(appID + '\t' + user + '\t' + vc + '\t' + gpu + '\t' + str(retries) + '\t' + status + '\n')
	if (flag):
		sb_time = submitTime - normalize_time
		#st_time = start_time - normalize_time
		ed_time = end_time - normalize_time
		jb = {"am.type":"philly", "job.start.ms":sb_time.total_seconds()*1000, "job.end.ms": ed_time.total_seconds()*1000, "job.queue.name":vc+"."+queue, "job.id":appID, "job.user":user, "job.gpu":gpu}
		json_out.write(json.dumps(jb) + "\n")
out.close()
json_out.close()
print "job dont have end time: " + str(job_dont_have_end_time)
