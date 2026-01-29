
### 
apt install mysql-client-core-8.0 -y

mkdir -p /home/mysql8/init

cat > /home/mysql8/init/01-create-db.sql << EOF
CREATE DATABASE IF NOT EXISTS worldquant 
DEFAULT CHARACTER SET utf8mb4 
DEFAULT COLLATE utf8mb4_unicode_ci;
EOF

# 赋予可读权限
chmod 644 /home/mysql8/init/01-create-db.sql

cp wqb.sql /home/mysql8/init/02-import-wqb.sql

sed -i '1i USE worldquant;' /home/mysql8/init/02-import-wqb.sql

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



###
## https://grafana.com/grafana/download
## docker run -d --name=grafana -p 3000:3000 grafana/grafana-enterprise

mkdir -p /home/grafana/{provisioning/dashboards,dashboards}

cp 01.json /home/grafana/dashboards/
cp 02.json /home/grafana/dashboards/
cp 03.json /home/grafana/dashboards/
cp 04.json /home/grafana/dashboards/
cp 05.json /home/grafana/dashboards/

# 统一设置目录和文件权限（避免容器读取失败，关键步骤）
chmod -R 755 /home/grafana/
chown -R 472:472 /home/grafana/  # 472 是 Grafana 容器内默认用户 UID

cat > /home/grafana/provisioning/dashboards/dashboard.yml << EOF
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''             # 所有 dashboard 导入到根文件夹（可自定义，比如 '业务监控'）
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards  # 扫描该目录下所有兼容 JSON 文件
      foldersFromFilesStructure: false
EOF


docker run -d \
--name=grafana \
--restart=always \
-p 3000:3000 \
-v /home/grafana/provisioning:/etc/grafana/provisioning \
-v /home/grafana/dashboards:/var/lib/grafana/dashboards \
grafana/grafana-enterprise

##### 后续若新增 05.json，只需复制到 /home/grafana/dashboards/ 目录，Grafana 会在 10 秒内（配置的 updateIntervalSeconds）自动识别并导入，无需重启容器；




