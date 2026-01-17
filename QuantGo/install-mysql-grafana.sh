
### 
apt install mysql-client-core-8.0

docker run  -d  \
--name mysql8 \
--privileged=true \
--restart=always \
-p 3310:3306 \
-v /home/mysql8/data:/var/lib/mysql \
-v /home/mysql8/config:/etc/mysql/conf.d  \
-v /home/mysql8/logs:/logs \
-e MYSQL_ROOT_PASSWORD=123456 \
-e TZ=Asia/Shanghai mysql \
--lower_case_table_names=1


###
## https://grafana.com/grafana/download
docker run -d --name=grafana -p 3000:3000 grafana/grafana-enterprise


