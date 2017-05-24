USERNAME="fareast\v-wencxi" 

PASSWORD=""

CLUSTER="gcr" 

VC="all"  

FINISHED_NUM="2000"     

CMD="https://master.$CLUSTER.philly.selfhost.corp.microsoft.com/restapi/list?" 

CMD+="clusterId=$CLUSTER&" 

CMD+="vcId=$VC&"

CMD+="userName=all&"

CMD+="status=t&"

CMD+="numFinished=$FINISHED_NUM" 

rm temp.json
echo $CMD
curl -k --ntlm --user "$USERNAME:$PASSWORD" "$CMD" >> temp.json 

python -m json.tool temp.json >> "$CLUSTER""_joblist.json"

