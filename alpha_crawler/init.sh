### 
apt install mysql-client-core-8.0 -y

pip install mysql-connector-python --break-system-packages

mkdir -p /home/mysql8/init

### cat > /home/mysql8/init/01-create-db.sql << EOF
### CREATE DATABASE IF NOT EXISTS consultant_analytics 
### DEFAULT CHARACTER SET utf8mb4 
### DEFAULT COLLATE utf8mb4_unicode_ci;
### EOF

# 赋予可读权限
## chmod 644 /home/mysql8/init/01-create-db.sql

cp database_schema.sql /home/mysql8/init/02-import-wqb.sql

### sed -i '1i USE consultant_analytics;' /home/mysql8/init/02-import-wqb.sql

chmod 644 /home/mysql8/init/02-import-wqb.sql

docker run  -d  \
--name mysql8 \
--privileged=true \
--restart=always \
-p 3310:3306 \
-v /home/mysql8/data:/var/lib/mysql \
-v /home/mysql8/config:/etc/mysql/conf.d  \
-v /home/mysql8/logs:/logs \
-v /home/mysql8/init:/docker-entrypoint-initdb.d \
-e MYSQL_ROOT_PASSWORD=123456 \
-e TZ=Asia/Shanghai mysql \
--lower_case_table_names=1


####### mysql -h 172.30.1.2 -uroot -P 3310 -p
