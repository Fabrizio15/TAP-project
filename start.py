import os 

os.system("docker build ./logstash -t tap:logstash")
os.system("docker build ./kafka -t tap:kafka")
os.system("docker build ./spark -t tap:spark")
os.system("docker build ./data_visualization/kibana -t tap:kibana")