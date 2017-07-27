import datetime
import json

normalize_time = datetime.datetime(2017, 5, 12, 0, 0, 0, 0)
normalize_end_time = datetime.datetime(2017, 5, 13, 0, 0, 0, 0)

try:
	with open("temp.json", 'r') as content:
		job_status_repo = json.loads(content.read())
except:
	job_status_repo = {}

out = open("trace_input.txt", 'w')
json_out = open("sls_input.json", "w")
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
unreasonable = 0
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
	
	submitTime = datetime.datetime.strptime(arriveTime, '%Y-%m-%d %H:%M:%S')
	if (submitTime < normalize_time):
		continue;
	
	if ((len(endTime) < len(startTime)) and (len(startTime) > (retries + 1))):
		tmp += 1
		weird_app = weird_app + appID + "\n"
	total_jobs += 1
	if (mapping == []):
		kill_in_queue += 1
		continue;
	end_start_flag = 1
	if (len(startTime) == len(endTime)):
		flag = 0
		for i in range(len(startTime)):
			start = datetime.datetime.strptime(startTime[i], '%Y-%m-%d %H:%M:%S')
			end = datetime.datetime.strptime(endTime[i], '%Y-%m-%d %H:%M:%S')
			if (end < start):
				end_start_flag = 0
				break
			gap = end - start
			if (gap.total_seconds() > 5):
				flag = 1
				break
		if (end_start_flag == 0):
			end_greater_start += 1
			continue
		if (flag == 0):
			all_retries_within_five_seconds += 1
			continue
	for i in range(retries + 1):
		total_retries += 1
		at = ""
		if (status == "Killed" and len(startTime) == len(endTime) and len(startTime) == retries and i ==len(startTime)):
			kill_in_queue_retries += 1
			continue
		if (status != "Failed" and len(startTime) == len(endTime) and len(startTime) == retries and i ==len(startTime)):
			fail_in_queue_retries += 1
			continue
		if (status != "Pass" and len(startTime) == len(endTime) and len(startTime) == retries and i ==len(startTime)):
			pass_in_queue_retries += 1
			continue
		if (len(endTime) == len(startTime) and i >= len(endTime)):
			missing_retries += 1
			continue;
		if ((len(endTime) < retries + 1 and i >= len(endTime)) or (len(startTime) < retries + 1 and i >= len(startTime))):
			if (i == 0):
				at = str(arriveTime)
			else:
				at = "-1"
			startTimeStr = "-1"
			endTimeStr = "-1"
			weird_retries += 1 
		else:
			if (i == 0):
				at = str(arriveTime)
			else:
				at = str(endTime[i - 1])
			startTimeStr = str(startTime[i])
			endTimeStr = str(endTime[i])
		if (at == "-1"):
			gpumap = ""
			continue
		else:
			gpumap = str(mapping[i])
		out.write(appID + "\t" + status + "\t" + str(i) + "\t" + str(retries) + "\t" + user + "\t" + vc + "\t" + queue + "\t" + str(gpu) + "\t" + str(arriveTime) + "\t" + at + "\t" + startTimeStr + "\t" + endTimeStr + "\t" + gpumap + "\n")
		if (at == "-1" or startTimeStr == "-1" or endTimeStr == "-1"):
		    unreasonable += 1
		    continue
		submitT = (datetime.datetime.strptime(at, '%Y-%m-%d %H:%M:%S') - normalize_time).total_seconds() * 1000
		startT = (datetime.datetime.strptime(startTimeStr, '%Y-%m-%d %H:%M:%S') - normalize_time).total_seconds() * 1000
		endT = (datetime.datetime.strptime(endTimeStr, '%Y-%m-%d %H:%M:%S') - normalize_time).total_seconds() * 1000
		if (endT < startT):
		    unreasonable += 1
		    continue
		json_item={"am.type":"philly", "job.submit.ms":int(submitT),"job.start.ms":int(startT), "job.end.ms":int(endT), "job.queue.name":vc+"."+queue, "job.id":appID+"_"+str(i), "job.user":user, "job.gpu":str(gpu) }
		if (datetime.datetime.strptime(endTimeStr, '%Y-%m-%d %H:%M:%S')  < normalize_end_time):
		    json_out.write(json.dumps(json_item) + "\n")
	#
	print str(cnt) + appID
	cnt = cnt + 1
out.close()
json_out.close()

print "len(start) > len(end) and retries + 1 " + str(tmp)
print weird_app

print "total jobs: " + str(total_jobs)
print "kill in queue: " + str(kill_in_queue)
print "end greater start: " + str(end_greater_start)
print "all retries within five seconds: " + str(all_retries_within_five_seconds)
print "total retries: " + str(total_retries)
print "kill in queue retries: " +  str(kill_in_queue_retries)
print "fail in queue retries: " +  str(fail_in_queue_retries)
print "pass in queue retries: " +  str(pass_in_queue_retries)
print "missing retries: " + str(missing_retries)
print "weird retries: " + str(weird_retries)
print "unreasonable: " + str(unreasonable)
